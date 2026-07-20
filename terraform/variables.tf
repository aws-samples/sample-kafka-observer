# =============================================================================
# sample-kafka-observer / terraform — input variables
# =============================================================================

variable "region" {
  description = "AWS region to deploy into. Must match the region configured on the caller's AWS provider (a `check` block in main.tf enforces this). Default replicates the Tokyo POC."
  type        = string
  default     = "ap-northeast-1"
}

variable "az_ids" {
  description = "Optional list of exact AZ IDs (e.g. [\"apne1-az1\", \"apne1-az2\", \"apne1-az4\"]) to pin broker placement. AZ IDs are stable across accounts, unlike AZ names. Empty list = first 3 available AZs in the region."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.az_ids) == 0 || length(var.az_ids) == 3
    error_message = "az_ids must be empty (auto-select) or contain exactly 3 AZ IDs."
  }
}

variable "use_default_vpc" {
  description = "true = launch into the region's default VPC (no VPC resources created); false = create a dedicated VPC with 3 public subnets across 3 AZs."
  type        = bool
  default     = false
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated VPC. Ignored when use_default_vpc = true."
  type        = string
  default     = "10.42.0.0/16"
}

variable "instance_type" {
  description = "EC2 instance type for the 3 Kafka brokers (Graviton/arm64 required — AMI is AL2023 arm64). Tokyo POC used m7g.large."
  type        = string
  default     = "m7g.large"
}

variable "loadgen_instance_type" {
  description = "EC2 instance type for the loadgen/builder host. Empty = same as instance_type. Tip: tools/apply-and-build.sh compiles Kafka in ~1 min on m7g.xlarge vs several minutes on m7g.large."
  type        = string
  default     = ""
}

variable "broker_count" {
  description = "Number of Kafka broker instances. The observer topology (2 electable + 1 observer) needs 3; brokers are spread round-robin across the 3 selected AZs."
  type        = number
  default     = 3

  validation {
    condition     = var.broker_count >= 3
    error_message = "broker_count must be >= 3 for the observer POC topology."
  }
}

variable "kafka_version" {
  description = "Apache Kafka version to pre-download onto every node (vanilla binary from archive.apache.org; the observer patch is applied later on the builder via tools/apply-and-build.sh)."
  type        = string
  default     = "3.7.1"
}

variable "scala_version" {
  description = "Scala version embedded in the Kafka binary artifact name (kafka_<scala>-<kafka_version>.tgz)."
  type        = string
  default     = "2.13"
}

variable "mode" {
  description = "Cluster metadata mode: \"zk\" (ZooKeeper, fully supported by patch v0.3) or \"kraft\" (reserved for v0.5 — infrastructure identical, patch not yet available). Written to /etc/kafka-poc.env on every node."
  type        = string
  default     = "zk"

  validation {
    condition     = contains(["zk", "kraft"], var.mode)
    error_message = "mode must be \"zk\" or \"kraft\"."
  }
}

variable "key_pair_name" {
  description = "Name of an existing EC2 key pair in the target region, used for SSH access to all nodes."
  type        = string
}

variable "my_ip" {
  description = "Your public IPv4 address (without /32) allowed to SSH into all nodes. Empty = auto-detect via https://checkip.amazonaws.com at plan time."
  type        = string
  default     = ""

  validation {
    condition     = var.my_ip == "" || can(cidrnetmask("${var.my_ip}/32"))
    error_message = "my_ip must be empty or a valid IPv4 address (no CIDR suffix)."
  }
}

variable "root_volume_size_gb" {
  description = "Root EBS (gp3) volume size in GiB for every node. Kafka source checkout + Gradle build on the builder needs ~4 GiB; brokers need room for log segments."
  type        = number
  default     = 40
}

variable "name_prefix" {
  description = "Prefix applied to the Name tag of every resource."
  type        = string
  default     = "kafka-observer-poc"
}

variable "tags" {
  description = "Extra tags merged onto every resource."
  type        = map(string)
  default     = {}
}
