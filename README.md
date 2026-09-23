# Costfluent Kubernetes agent

The in-cluster agent that reports a Kubernetes cluster's resource usage to Costfluent, which
allocates the cost of the cluster's nodes to namespaces, workloads and labels. Install it with the
[Helm chart](https://github.com/costfluent/helm-charts/tree/main/costfluent-k8s-agent); the guide
is at <https://docs.costfluent.com/connect/kubernetes>.

## How it works

One replica, run as a StatefulSet:

1. Every polling interval it lists nodes and pods from the API server and reads each kubelet's
   `/metrics/resource` endpoint on port 10250 with its service-account token (RBAC
   `nodes/metrics get`), ten nodes at a time. No metrics-server is needed.
2. It integrates container CPU and memory usage, and max(request, usage), over the window in
   memory, and snapshots the open window to the data directory every five minutes so a restart
   does not lose it.
3. When a window closes (each UTC clock hour, plus a first window about two minutes after start) it
   buffers the report on the data directory and posts it, gzip JSON with a bearer token, to
   `POST {endpoint}/v1/kubernetes/reports`. The buffer keeps undelivered reports for 96 hours or
   50 MB, oldest dropped first.

`api/v1/report.go` is the whole wire contract, and `api/v1/testdata/report.json` is a golden
report. The chart README's table of what the agent reads and sends is kept in step with it.

## Configuration

Environment variables only; the chart sets each from an `agent.*` value.

| Variable | Chart value | Default | Meaning |
|---|---|---|---|
| `COSTFLUENT_TOKEN` | `agent.token` or `agent.secret.*` | required | Organization API token with the Report Kubernetes usage capability. |
| `COSTFLUENT_CLUSTER_ID` | `agent.clusterID` | required | The cluster's ID: `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`. |
| `COSTFLUENT_API_ENDPOINT` | `agent.apiEndpoint` | `https://api.costfluent.com` | `http://` is accepted with a warning, for a local receiver. |
| `COSTFLUENT_POLLING_INTERVAL` | `agent.pollingInterval` | `60` | Seconds between polls: 5, 10, 15, 30 or 60. |
| `COSTFLUENT_NODE_ADDRESS_TYPES` | `agent.nodeAddressTypes` | `InternalIP,InternalDNS,Hostname,ExternalIP,ExternalDNS` | Node address types tried, in order, to reach each kubelet. |
| `COSTFLUENT_KUBE_SKIP_TLS_VERIFY` | `agent.disableKubeTLSverify` | `false` | Skip verifying kubelet serving certificates. |
| `COSTFLUENT_ALLOWED_LABELS` | `agent.allowedLabels` | empty: every pod label | Comma-separated pod label keys to send. |
| `COSTFLUENT_ALLOWED_ANNOTATIONS` | `agent.allowedAnnotations` | empty: none | Comma-separated pod annotation keys to send, at most 10; values are cut to 100 characters. |
| `COSTFLUENT_COLLECT_NAMESPACE_LABELS` | `agent.collectNamespaceLabels` | `false` | Send namespace labels. |
| `COSTFLUENT_REPORT_HTTP_PROXY` | `agent.reportHTTPProxy` | none | HTTP proxy for report traffic only. |
| `COSTFLUENT_DATA_DIR` | `persist.mountPath` | `/data` (the chart sets `/var/lib/costfluent`) | Buffer, window snapshot and instance ID. Must be writable; the agent exits otherwise. |
| `COSTFLUENT_METRICS_PORT` | `service.port` | `9010` | Serves `/metrics`, `/healthz` and `/readyz`. |
| `COSTFLUENT_LOG_LEVEL` | `agent.logLevel` | `info` | `debug`, `info`, `warn` or `error`. |
| `COSTFLUENT_LOG_FORMAT` | `agent.extraEnv` | `json` | `json` or `console`. |

## Metrics and health

`/healthz` answers once the process runs; `/readyz` once the first poll has completed. `/metrics`
serves:

| Metric | Labels | Meaning |
|---|---|---|
| `costfluent_agent_info` | `version`, `cluster_id` | Always 1. |
| `costfluent_agent_node_scrape_total` | `result` (`ok`, `error`) | Kubelet scrapes. |
| `costfluent_agent_poll_duration_seconds` | | One poll across every node. |
| `costfluent_agent_reports_total` | `result` (`accepted`, `rejected`, `unauthorized`, `retry`) | Report submissions. |
| `costfluent_agent_report_bytes` | | Gzip report body size. |
| `costfluent_agent_buffer_reports`, `costfluent_agent_buffer_bytes` | | Reports waiting in the buffer. |
| `costfluent_agent_buffer_dropped_total` | `reason` (`age`, `size`, `rejected`) | Reports dropped from the buffer. |

## Development

```bash
make check    # gofmt, go vet, golangci-lint, go test -race, build
make e2e      # installs the chart into a kind cluster and asserts a report reaches a stub receiver
make docker   # builds ghcr.io/costfluent/k8s-agent
```

`make e2e` needs docker, kind, kubectl and helm. It reads the chart from `../helm-charts`, or from
`E2E_CHART`, and takes the kind node image from `KIND_NODE_IMAGE` when set. `E2E_KEEP_CLUSTER=1`
leaves the cluster running for inspection.

The agent reads only the in-cluster configuration, so it runs as a pod, never from a workstation.

## License

Apache License 2.0; see [LICENSE](./LICENSE).
