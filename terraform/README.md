# terraform/ — real-machine verification environment

Terraform module that recreates the **Tokyo POC topology** used to produce every
number in [`evidence/`](../evidence/): 3 Kafka brokers spread across 3 AZs plus
1 loadgen/builder host, all on Amazon Linux 2023 arm64 (Graviton).

```
                ap-northeast-1 (default)
 ┌─────────────┬─────────────┬─────────────┐
 │    AZ-a     │    AZ-b     │    AZ-c     │
 │  broker-1   │  broker-2   │  broker-3   │   m7g.large × 3
 │  loadgen    │             │             │   m7g.large × 1 (builder + clients)
 └─────────────┴─────────────┴─────────────┘
   self-referencing SG: all intra-cluster traffic; SSH only from your IP
```

## ⚠️ Cost warning

This spins up **4 × m7g.large On-Demand instances** (default). In ap-northeast-1
that is roughly **US$0.10/hr each ≈ US$0.41/hr ≈ US$10/day ≈ US$295/month** for
compute alone, plus 4 × 40 GiB gp3 EBS (~US$15/month) and a little egress.

**This is a throwaway POC environment. `terraform destroy` as soon as you are
done.** Nothing here is stateful that you would want to keep — all evidence is
captured as text/files you copy off the boxes.

```bash
terraform destroy   # do this. every time. check the console afterwards.
```

## What gets created

| Resource | Notes |
|---|---|
| VPC + 3 public subnets + IGW | Skipped when `use_default_vpc = true` |
| Security group | Self-referencing: any protocol between members; SSH (22) only from `my_ip` (auto-detected if empty) |
| 3 × broker EC2 | AL2023 arm64 (latest, via SSM parameter), one per AZ, `Name = *-broker-{1..3}` |
| 1 × loadgen EC2 | Same AMI/SG; used as build host for `tools/apply-and-build.sh` and as client/loadgen |

Each node's user_data installs **JDK 17 (Corretto devel, includes javac)** and
**git**, downloads the **vanilla** Kafka binary to `/opt/kafka`, and writes node
identity to `/etc/kafka-poc.env`. It deliberately does **not**:

- apply the observer patch — you do that on the builder with
  [`tools/apply-and-build.sh`](../tools/apply-and-build.sh), keeping one
  canonical patch/build path (the `.patch` file is the distributed artifact,
  never pre-built jars);
- start ZooKeeper/Kafka — broker configs, `observer.ids`, and startup order are
  runbook steps ([`docs/runbooks/`](../docs/runbooks/)).

## Usage

```bash
cd terraform/examples/tokyo-poc
terraform init
terraform apply -var key_pair_name=my-tokyo-key

# machine-readable cluster map for the pytest aws backend
terraform output -json cluster_json > cluster.json

# SSH commands are printed for you
terraform output ssh_commands
```

Then follow the runbooks: build the patched jar on the loadgen/builder node,
copy `kafka_2.13-3.7.1.jar` (+ storage jar) to the brokers, write
`/opt/kafka/observer.ids`, configure and start the cluster.

> ⚠️ **ZooKeeper-mode gotcha (v0.3):** with the ZK controller, topics **created
> after** an observer restart may require a broker restart to pick up observer
> gating for new partitions — see the prominent note in
> [`docs/multi-version.md`](../docs/multi-version.md) before filing a bug.

## Inputs (most useful)

| Variable | Default | Description |
|---|---|---|
| `region` | `ap-northeast-1` | Must match the caller's provider region (enforced by a `check` block) |
| `key_pair_name` | — (required) | Existing EC2 key pair for SSH |
| `instance_type` | `m7g.large` | Brokers (arm64 only — AMI is AL2023 arm64) |
| `loadgen_instance_type` | `""` = same as brokers | Tip: `m7g.xlarge` builds the patched jar in ~1 min |
| `kafka_version` | `3.7.1` | Vanilla binary pre-downloaded on every node |
| `mode` | `zk` | `zk` or `kraft` (infra identical — the mode only changes how you configure/start Kafka on the nodes) |
| `az_ids` | `[]` = first 3 AZs | Pin exact AZ IDs, e.g. `["apne1-az1","apne1-az2","apne1-az4"]` |
| `use_default_vpc` | `false` | `true` reuses the region's default VPC/subnets |
| `my_ip` | `""` = auto-detect | Public IPv4 allowed to SSH |

See [`variables.tf`](variables.tf) for the full list; all outputs are in
[`outputs.tf`](outputs.tf) (`cluster_json`, `ssh_commands`, per-role IPs).

## Security posture (POC-grade, know what you're getting)

- Nodes have **public IPs** (simple SSH access, no bastion/SSM session plumbing).
  Only port 22 from your IP is exposed; Kafka/ZK ports are cluster-internal.
- No EBS encryption toggles, no IMDSv2 hardening options, no detailed
  monitoring — this module optimizes for reproducing the POC cheaply, not for
  production. Do not host real data here.

## Destroy checklist

1. Copy any evidence output off the boxes (`scp`/`rsync`).
2. `terraform destroy` from the same directory/state you applied in.
3. Verify in the console: EC2 instances terminated, VPC gone (if created),
   no leftover EBS volumes.
