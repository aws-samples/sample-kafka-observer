# =============================================================================
# sample-kafka-observer / terraform — Tokyo-POC verification environment
#
# Topology (replicates the real-machine POC used for all evidence/ files):
#   - 3 Kafka broker EC2 instances spread across 3 AZs (broker ids 1..3)
#   - 1 loadgen/builder EC2 instance (compiles the patched jar with
#     tools/apply-and-build.sh and drives producers/consumers)
#   - AL2023 arm64 AMI (resolved via SSM public parameter), default m7g.large
#   - Self-referencing security group: unrestricted traffic inside the
#     cluster, SSH only from var.my_ip
#
# Nodes come up with vanilla Kafka pre-downloaded but NOT patched and NOT
# started — patching/deployment stays in tools/apply-and-build.sh and the
# runbooks so the on-box workflow is identical to a manual install.
# =============================================================================

# --- Region guard -----------------------------------------------------------
# The module cannot set the provider region itself (callers own the provider),
# but it can refuse to plan when the caller's provider disagrees with
# var.region — a silent mismatch would put the POC in the wrong region.

data "aws_region" "current" {}

check "region_matches_provider" {
  assert {
    condition     = data.aws_region.current.name == var.region
    error_message = "Provider region (${data.aws_region.current.name}) does not match var.region (${var.region}). Configure the aws provider with region = var.region."
  }
}

# --- AZ selection ------------------------------------------------------------

data "aws_availability_zones" "available" {
  state = "available"

  # When az_ids is set, restrict to exactly those zone IDs (stable across
  # accounts); otherwise take whatever the region offers.
  dynamic "filter" {
    for_each = length(var.az_ids) > 0 ? [1] : []
    content {
      name   = "zone-id"
      values = var.az_ids
    }
  }
}

locals {
  az_names = slice(data.aws_availability_zones.available.names, 0, 3)
  az_ids   = slice(data.aws_availability_zones.available.zone_ids, 0, 3)

  loadgen_instance_type = var.loadgen_instance_type != "" ? var.loadgen_instance_type : var.instance_type

  ssh_cidr = var.my_ip != "" ? "${var.my_ip}/32" : "${chomp(data.http.my_ip[0].response_body)}/32"

  common_tags = merge(
    {
      Project = "sample-kafka-observer"
      Purpose = "kafka-observer-poc"
    },
    var.tags,
  )
}

# --- SSH source IP auto-detection (only when my_ip not given) ----------------

data "http" "my_ip" {
  count = var.my_ip == "" ? 1 : 0
  url   = "https://checkip.amazonaws.com"
}

# --- Networking: default VPC or dedicated VPC --------------------------------

data "aws_vpc" "default" {
  count   = var.use_default_vpc ? 1 : 0
  default = true
}

# Default VPC path: pick the default-for-AZ subnet in each selected AZ.
data "aws_subnet" "default" {
  count             = var.use_default_vpc ? 3 : 0
  vpc_id            = data.aws_vpc.default[0].id
  availability_zone = local.az_names[count.index]
  default_for_az    = true
}

# Dedicated VPC path: minimal public VPC (POC-grade; no NAT, no private tier).
resource "aws_vpc" "this" {
  count                = var.use_default_vpc ? 0 : 1
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.common_tags, { Name = "${var.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "this" {
  count  = var.use_default_vpc ? 0 : 1
  vpc_id = aws_vpc.this[0].id

  tags = merge(local.common_tags, { Name = "${var.name_prefix}-igw" })
}

resource "aws_subnet" "public" {
  count                   = var.use_default_vpc ? 0 : 3
  vpc_id                  = aws_vpc.this[0].id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = local.az_names[count.index]
  map_public_ip_on_launch = true

  tags = merge(local.common_tags, {
    Name = "${var.name_prefix}-public-${local.az_names[count.index]}"
  })
}

resource "aws_route_table" "public" {
  count  = var.use_default_vpc ? 0 : 1
  vpc_id = aws_vpc.this[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[0].id
  }

  tags = merge(local.common_tags, { Name = "${var.name_prefix}-public-rt" })
}

resource "aws_route_table_association" "public" {
  count          = var.use_default_vpc ? 0 : 3
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

locals {
  vpc_id = var.use_default_vpc ? data.aws_vpc.default[0].id : aws_vpc.this[0].id

  # subnet id per AZ index (0..2), regardless of which VPC path is active
  subnet_ids = var.use_default_vpc ? data.aws_subnet.default[*].id : aws_subnet.public[*].id
}

# --- Security group -----------------------------------------------------------

resource "aws_security_group" "cluster" {
  name_prefix = "${var.name_prefix}-"
  description = "Kafka observer POC: intra-cluster all traffic, SSH from operator IP"
  vpc_id      = local.vpc_id

  # Full intra-cluster connectivity (Kafka 9092, ZK 2181/2888/3888, JMX,
  # KRaft 9093, iperf, ...) — self-referencing keeps it closed to the world.
  ingress {
    description = "All traffic between cluster members"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "SSH from operator IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.ssh_cidr]
  }

  egress {
    description = "All outbound (package installs, Kafka source clone)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "${var.name_prefix}-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

# --- AMI: Amazon Linux 2023 arm64 via SSM public parameter --------------------

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

# --- EC2 instances -------------------------------------------------------------

resource "aws_instance" "broker" {
  count = var.broker_count

  ami                         = data.aws_ssm_parameter.al2023_arm64.value
  instance_type               = var.instance_type
  key_name                    = var.key_pair_name
  subnet_id                   = local.subnet_ids[count.index % 3]
  vpc_security_group_ids      = [aws_security_group.cluster.id]
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  user_data = templatefile("${path.module}/templates/user_data.sh.tpl", {
    node_role     = "broker"
    broker_id     = count.index + 1
    kafka_version = var.kafka_version
    scala_version = var.scala_version
    mode          = var.mode
  })

  tags = merge(local.common_tags, {
    Name     = "${var.name_prefix}-broker-${count.index + 1}"
    Role     = "broker"
    BrokerId = count.index + 1
  })
}

resource "aws_instance" "loadgen" {
  ami                         = data.aws_ssm_parameter.al2023_arm64.value
  instance_type               = local.loadgen_instance_type
  key_name                    = var.key_pair_name
  subnet_id                   = local.subnet_ids[0]
  vpc_security_group_ids      = [aws_security_group.cluster.id]
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  user_data = templatefile("${path.module}/templates/user_data.sh.tpl", {
    node_role     = "loadgen"
    broker_id     = 0
    kafka_version = var.kafka_version
    scala_version = var.scala_version
    mode          = var.mode
  })

  tags = merge(local.common_tags, {
    Name = "${var.name_prefix}-loadgen"
    Role = "loadgen"
  })
}
