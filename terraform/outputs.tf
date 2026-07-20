# =============================================================================
# sample-kafka-observer / terraform — outputs
# =============================================================================

output "cluster_json" {
  description = "Machine-readable cluster map (broker id -> private/public IP + AZ, plus loadgen) for the pytest aws backend. Consume with: terraform output -json cluster_json > cluster.json"
  value = {
    region        = var.region
    mode          = var.mode
    kafka_version = var.kafka_version
    brokers = [
      for i, b in aws_instance.broker : {
        broker_id   = i + 1
        instance_id = b.id
        private_ip  = b.private_ip
        public_ip   = b.public_ip
        az          = b.availability_zone
        az_id       = local.az_ids[i % 3]
      }
    ]
    loadgen = {
      instance_id = aws_instance.loadgen.id
      private_ip  = aws_instance.loadgen.private_ip
      public_ip   = aws_instance.loadgen.public_ip
      az          = aws_instance.loadgen.availability_zone
    }
  }
}

output "broker_private_ips" {
  description = "Broker private IPs in broker-id order (1..N)."
  value       = aws_instance.broker[*].private_ip
}

output "broker_public_ips" {
  description = "Broker public IPs in broker-id order (1..N)."
  value       = aws_instance.broker[*].public_ip
}

output "loadgen_public_ip" {
  description = "Public IP of the loadgen/builder host."
  value       = aws_instance.loadgen.public_ip
}

output "ssh_commands" {
  description = "Ready-to-paste SSH commands for every node (assumes the key pair's private key is at ~/.ssh/<key_pair_name>.pem)."
  value = concat(
    [
      for i, b in aws_instance.broker :
      "ssh -i ~/.ssh/${var.key_pair_name}.pem ec2-user@${b.public_ip}  # broker-${i + 1} (${b.availability_zone})"
    ],
    [
      "ssh -i ~/.ssh/${var.key_pair_name}.pem ec2-user@${aws_instance.loadgen.public_ip}  # loadgen/builder (${aws_instance.loadgen.availability_zone})"
    ],
  )
}

output "security_group_id" {
  description = "Cluster security group id (useful for attaching extra test instances)."
  value       = aws_security_group.cluster.id
}

output "vpc_id" {
  description = "VPC the cluster runs in (default VPC or the dedicated one)."
  value       = local.vpc_id
}
