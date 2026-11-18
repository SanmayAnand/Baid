# deploy/

## Local stack (`docker-compose.yml`)

The backends BAiD sits in front of, plus metrics, for development and benchmarks.

| Service | Image | Ports (127.0.0.1 only) | Used for |
| --- | --- | --- | --- |
| Neo4j | `neo4j:5.26-community` (LTS) | 7474 browser, 7687 Bolt | Graph memory backend (Bolt adapter, T11) |
| SeaweedFS | `chrislusf/seaweedfs:3.97` | 8333 S3 API | Object-store memory backend (S3 adapter, T12) |
| Prometheus | `prom/prometheus:v3.5.0` | 9090 | Scrapes BAiD's admin port `/metrics` (T21) |
| Grafana | `grafana/grafana:12.1.1` | 3000 | Dashboards (T35); anonymous read-only access |

```bash
just up      # start and wait until every service is healthy
just ps      # status
just logs    # follow logs (just logs neo4j for one service)
just down    # stop; data volumes are kept
```

To wipe all local data as well, run `docker compose -f deploy/docker-compose.yml down -v`.
This deletes the Neo4j, SeaweedFS, Prometheus and Grafana volumes.

### Credentials

Development defaults. Neo4j and Grafana can be overridden in `deploy/.env` (copy
`.env.example`; `.env` is not committed); the S3 keys are in `deploy/seaweedfs/s3.json`:

| Service | User | Password |
| --- | --- | --- |
| Neo4j | `neo4j` | `baid-dev-password` |
| SeaweedFS (S3) | access key `baid` | secret key `baid-dev-password` (in `seaweedfs/s3.json`) |
| Grafana | `admin` | `baid-dev-password` |

Every port is bound to `127.0.0.1`, so none of this is reachable from the network.

### BAiD itself

BAiD runs on the host (`cargo run -p server -- -c baid.toml`). Prometheus reaches its admin
port at `host.docker.internal:7071`; the `baid` target shows as down until `/metrics`
exists (T21).

### Why SeaweedFS, not MinIO

The spec names MinIO, but MinIO no longer publishes container images (the `minio/minio`
repository is gone from Docker Hub and quay.io). SeaweedFS is Apache-2.0, runs as one
container and speaks the S3 API, which is all the S3 adapter uses. Any S3-compatible store
works the same way.

## Later

Helm charts for the sidecar and shared-service topologies, and the Grafana dashboard JSON.
