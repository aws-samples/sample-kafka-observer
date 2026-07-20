# =============================================================================
# sample-kafka-observer — minimal example: Tokyo POC topology
#
# 3 brokers across 3 AZs + 1 loadgen/builder in ap-northeast-1,
# exactly the environment behind the numbers in evidence/.
#
# Usage:
#   cd terraform/examples/tokyo-poc
#   terraform init
#   terraform plan  -var key_pair_name=my-tokyo-key
#   terraform apply -var key_pair_name=my-tokyo-key
#   terraform output -json cluster_json > cluster.json
#   ...run the POC (see docs/runbooks)...
#   terraform destroy -var key_pair_name=my-tokyo-key   # DO NOT FORGET
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "key_pair_name" {
  description = "Existing EC2 key pair in ap-northeast-1 for SSH access."
  type        = string
}

provider "aws" {
  region = "ap-northeast-1"
}

module "kafka_observer_poc" {
  source = "../.."

  region        = "ap-northeast-1"
  key_pair_name = var.key_pair_name

  # Everything below shows the defaults — uncomment to override.
  # instance_type         = "m7g.large"
  # loadgen_instance_type = "m7g.xlarge" # faster apply-and-build.sh (~1 min)
  # kafka_version         = "3.7.1"
  # mode                  = "zk"
  # use_default_vpc       = false
  # az_ids                = ["apne1-az1", "apne1-az2", "apne1-az4"]
  # my_ip                 = "203.0.113.10" # empty = auto-detect
}

output "cluster_json" {
  description = "Broker/loadgen IP + AZ map for the pytest aws backend."
  value       = module.kafka_observer_poc.cluster_json
}

output "ssh_commands" {
  description = "Ready-to-paste SSH commands."
  value       = module.kafka_observer_poc.ssh_commands
}
