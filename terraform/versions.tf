# =============================================================================
# sample-kafka-observer / terraform — provider requirements
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Used only to auto-detect the caller's public IP for the SSH ingress rule
    # when var.my_ip is left empty.
    http = {
      source  = "hashicorp/http"
      version = "~> 3.4"
    }
  }
}
