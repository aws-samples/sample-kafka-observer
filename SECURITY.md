# Security Issue Notifications

If you discover a potential security issue in this project, we ask that you
notify AWS/Amazon Security via our
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/)
or directly via email to [aws-security@amazon.com](mailto:aws-security@amazon.com).

Please do **not** create a public GitHub issue for security vulnerabilities.

## Scope notes for this project

- This repository distributes **source patches only** — no pre-built binaries.
  Vulnerabilities in Apache Kafka itself should be reported to the
  [Apache Kafka security process](https://kafka.apache.org/project-security),
  not here.
- The observer mechanism reads a broker-local file
  (`/opt/kafka/observer.ids`). Whoever can write that file can change
  election eligibility of replicas — protect it with the same filesystem
  permissions as `server.properties`.
- The operational scripts (`scripts/`, `tools/`) execute administrative Kafka
  commands; run them only from trusted operator hosts.
