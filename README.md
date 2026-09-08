# Costfluent Kubernetes Agent

Lightweight in-cluster agent that collects container metrics and reports to Costfluent for cost allocation.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│  ┌──────────────────────┐                                   │
│  │  Costfluent Agent     │  StatefulSet (1 replica)          │
│  │  ┌────────────────┐  │                                   │
│  │  │ Scraper        │──┼──► metrics-server (all nodes)     │
│  │  │ (every 60s)    │  │                                   │
│  │  └────────────────┘  │                                   │
│  │  ┌────────────────┐  │                                   │
│  │  │ Metadata       │──┼──► kube-apiserver (pods, nodes)   │
│  │  │ Collector      │  │                                   │
│  │  └────────────────┘  │                                   │
│  │  ┌────────────────┐  │                                   │
│  │  │ Reporter       │──┼──► Costfluent API (hourly)         │
│  │  └────────────────┘  │                                   │
│  │  ┌────────────────┐  │                                   │
│  │  │ PV Buffer      │  │  Crash recovery, offline buffer   │
│  │  └────────────────┘  │                                   │
│  └──────────────────────┘                                   │
└─────────────────────────────────────────────────────────────┘
```

## Features

- Container CPU/memory metrics collection via metrics-server
- Pod metadata (labels, annotations, controller info)
- Node capacity and pricing info
- Spot/preemptible instance detection
- On-prem pricing via node annotations
- Hourly batch reporting with gzip compression
- PV-backed buffer for offline resilience
- Prometheus metrics endpoint

## Requirements

- Kubernetes 1.24+
- metrics-server installed
- Network access to Costfluent API

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `COSTFLUENT_TOKEN` | API token (required) | - |
| `COSTFLUENT_CLUSTER_ID` | Cluster identifier (required) | - |
| `COSTFLUENT_CLUSTER_NAME` | Display name | - |
| `COSTFLUENT_API_ENDPOINT` | API URL | `https://api.costfluent.com` |
| `COSTFLUENT_POLLING_INTERVAL` | Scrape interval | `60s` |
| `COSTFLUENT_REPORTING_INTERVAL` | Report interval | `3600s` |
| `COSTFLUENT_HEARTBEAT_INTERVAL` | Heartbeat interval | `300s` |
| `COSTFLUENT_NAMESPACE_EXCLUDE` | Excluded namespaces | `kube-system,kube-public` |
| `COSTFLUENT_LOG_LEVEL` | Log level | `info` |
| `COSTFLUENT_LOG_FORMAT` | Log format (json/console) | `json` |
| `COSTFLUENT_DATA_DIR` | Buffer directory | `/data` |
| `COSTFLUENT_METRICS_PORT` | Prometheus port | `9010` |

## Building

### Prerequisites

- Go 1.23+
- Docker (for container builds)
- golangci-lint (for linting)

### Using Makefile

```bash
make build          # Build binary to bin/
make test           # Run tests with race detector
make lint           # Run golangci-lint
make docker         # Build Docker image
make check          # Run all checks (fmt, vet, lint, test)
make help           # Show all targets
```

### Manual Build

```bash
# Build binary
CGO_ENABLED=0 go build -o bin/costfluent-k8s-agent ./cmd/agent

# Build with version info
CGO_ENABLED=0 go build \
  -ldflags "-s -w -X main.Version=1.0.0 -X main.GitCommit=$(git rev-parse --short HEAD)" \
  -o bin/costfluent-k8s-agent ./cmd/agent

# Cross-compile for Linux
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/costfluent-k8s-agent-linux ./cmd/agent
```

### Docker Build

```bash
# Build image
docker build -t ghcr.io/costfluent/k8s-agent:latest .

# Build with version
docker build \
  --build-arg VERSION=1.0.0 \
  --build-arg GIT_COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t ghcr.io/costfluent/k8s-agent:1.0.0 .

# Push to registry
docker push ghcr.io/costfluent/k8s-agent:1.0.0
```

## Development

```bash
# Install dependencies
go mod tidy

# Run locally (requires kubeconfig or in-cluster)
export COSTFLUENT_TOKEN=dev_token
export COSTFLUENT_CLUSTER_ID=dev_cluster
export COSTFLUENT_LOG_FORMAT=console
export COSTFLUENT_LOG_LEVEL=debug
go run ./cmd/agent

# Run tests
go test -v ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Format code
go fmt ./...

# Lint
golangci-lint run ./...
```

## Project Structure

```
k8s-agent/
├── cmd/agent/
│   └── main.go              # Entry point, orchestration
├── api/v1/
│   └── report.go            # API payload types
├── internal/
│   ├── config/
│   │   └── config.go        # Configuration loading
│   ├── collector/
│   │   ├── kubelet.go       # Metrics collection from metrics-server
│   │   ├── metadata.go      # Pod/node metadata from kube-apiserver
│   │   └── aggregator.go    # Time-window metric aggregation
│   ├── health/
│   │   └── health.go        # Health checks (K8s API, scrape status)
│   ├── metrics/
│   │   └── metrics.go       # Prometheus metrics registration
│   ├── reporter/
│   │   └── http.go          # Costfluent API client
│   └── storage/
│       └── buffer.go        # PV-backed persistent buffer
├── Dockerfile               # Multi-stage build (scratch)
├── Makefile                 # Build, test, lint targets
├── go.mod
└── README.md
```

## Data Collected

| Category | Fields | Source |
|----------|--------|--------|
| Container Metrics | cpu_usage, memory_usage | metrics-server |
| Pod Metadata | name, namespace, labels, annotations | kube-apiserver |
| Controller Info | controller_name, controller_kind | ownerReferences |
| Node Info | instance_type, region, zone, capacity | kube-apiserver |
| Node Pricing | vcpu_rate, ram_rate (on-prem) | node annotations |

## On-Prem Pricing

For clusters without cloud provider pricing, add annotations to nodes:

```yaml
apiVersion: v1
kind: Node
metadata:
  name: worker-1
  annotations:
    costfluent.com/vcpu-hourly-rate: "0.05"      # $0.05/vCPU/hour
    costfluent.com/ram-gb-hourly-rate: "0.007"   # $0.007/GB/hour
    costfluent.com/gpu-hourly-rate: "1.50"       # $1.50/GPU/hour
```

## Metrics & Health

### Prometheus Metrics

Available at `:9010/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `costfluent_agent_info` | Gauge | Agent version, cluster info (labels) |
| `costfluent_agent_scrape_duration_seconds` | Histogram | Scrape duration |
| `costfluent_agent_pods_scraped_total` | Counter | Pods scraped |
| `costfluent_agent_nodes_scraped_total` | Counter | Nodes scraped |
| `costfluent_agent_samples_collected_total` | Counter | Metric samples collected |
| `costfluent_agent_report_success_total` | Counter | Successful reports |
| `costfluent_agent_report_failure_total` | Counter | Failed reports |
| `costfluent_agent_report_size_bytes` | Histogram | Compressed report size |
| `costfluent_agent_report_duration_seconds` | Histogram | Report send duration |
| `costfluent_agent_buffer_size_bytes` | Gauge | Buffer size |
| `costfluent_agent_buffer_report_count` | Gauge | Buffered reports count |
| `costfluent_agent_heartbeat_success_total` | Counter | Successful heartbeats |
| `costfluent_agent_heartbeat_failure_total` | Counter | Failed heartbeats |

### Health Endpoints

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `/healthz` | Liveness probe | 200 if K8s API reachable |
| `/readyz` | Readiness probe | JSON status with K8s/API/scrape state |

```bash
# Check readiness
curl http://localhost:9010/readyz | jq
```

## RBAC Permissions

The agent requires read-only access:

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes", "pods", "namespaces", "persistentvolumes", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["apps"]
    resources: ["deployments", "replicasets", "statefulsets", "daemonsets"]
    verbs: ["get", "list"]
  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["get", "list"]
  - apiGroups: ["metrics.k8s.io"]
    resources: ["pods", "nodes"]
    verbs: ["get", "list"]
```

## Troubleshooting

**Agent not sending data:**
```bash
# Check logs
kubectl -n costfluent logs -l app.kubernetes.io/name=costfluent-agent -f

# Verify token
kubectl -n costfluent get secret costfluent-agent -o jsonpath='{.data.token}' | base64 -d
```

**Metrics not available:**
```bash
# Check metrics-server
kubectl top pods -A

# If missing, install metrics-server
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

**Buffer growing:**
```bash
# Check buffer stats
kubectl -n costfluent exec -it costfluent-agent-0 -- ls -la /data/pending/
```

## License

Proprietary - Costfluent, Inc.
