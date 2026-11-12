# Costfluent Kubernetes agent

The in-cluster agent that reports a Kubernetes cluster's resource usage to Costfluent, which
allocates the cost of the cluster's nodes to namespaces, workloads and labels. Install it with the
[Helm chart](https://github.com/costfluent/helm-charts/tree/main/costfluent-k8s-agent); the guide
is at <https://docs.costfluent.com/connect/kubernetes>.

## How it works

One replica, run as a StatefulSet. It reports with a cluster token, which names one cluster and
may report only that one. At start it reads the token with `GET {endpoint}/v1/me`, takes its
cluster from it, and exits, with the reason in its log, when the API refuses the token, it is not a
cluster token, or `COSTFLUENT_CLUSTER_ID` is set to another cluster, since every report would be
refused. It keeps the token's cluster in `cluster-id` in its data directory. An API it cannot
reach at start is logged and left to the report retries when `COSTFLUENT_CLUSTER_ID` is set, or
else when an earlier start kept the cluster; knowing neither, the agent exits and asks again when
the pod restarts.

A cluster token does not expire. A revoked token, or one whose minting account is disabled or
deleted, is refused with 401: the agent stops sending, keeps its buffered reports, and exits at its
next start until it gets a valid token. Rotate it as the chart README's
[Rotate the token](https://github.com/costfluent/helm-charts/tree/main/costfluent-k8s-agent#rotate-the-token)
describes.

1. Every polling interval it lists nodes and pods from the API server and reads each kubelet's
   `/metrics/resource` endpoint on port 10250 with a token for a second service account whose only
   permission is `nodes/metrics get` (`COSTFLUENT_KUBELET_SERVICE_ACCOUNT`), never with its own,
   which reads every pod. It requests that token through the TokenRequest API, valid for an hour
   and renewed before it expires; its RBAC allows `create` on `serviceaccounts/token` for that one
   account only. A token request that fails leaves that poll's nodes unscraped, and the next poll
   asks again. Under a chart that creates no such account, the scrapes carry the agent's own token.
   No metrics-server is needed. It also reads the persistent volumes and their storage classes,
   and each pod's claims, so the volumes' cost can go to the pods mounting them; storage that
   cannot be read leaves volumes out of the reports and holds nothing else back.
2. It integrates container CPU and memory usage, and max(request, usage), over the window in
   memory, and snapshots the open window to the data directory every five minutes so a restart
   does not lose it. A node whose kubelet cannot be read in an interval, and its pods, are left
   out of that interval rather than counted as zero usage, so a node that is never readable (no
   address, no `nodes/metrics` permission, a virtual node without `/metrics/resource`) is not
   reported, and `costfluent_agent_node_scrape_total{result="error"}` shows it. The report's
   `unscraped_nodes` counts, by reason (`tls`, `unreachable`, `unauthorized`, `other`), the nodes
   seen in the window whose kubelet no poll of it could read, and a window in which no node could
   be read is still sent, without nodes, so Costfluent can show why its usage is missing.
3. When a window closes (each UTC clock hour, plus a first window about two minutes after start) it
   buffers the report on the data directory and posts it, gzip JSON with a bearer token, to
   `POST {endpoint}/v1/kubernetes/reports`. The buffer keeps undelivered reports for 96 hours or
   50 MB, oldest dropped first. A report refused with 400, 409, 410 or 413 is dropped, except one
   whose window the API's clock has not reached yet; any other answer, including 422 for a
   `schema_version` the API does not read yet, keeps it to retry.

`api/v1/report.go` is the whole wire contract, and `api/v1/testdata/report.json` is a golden
report. The chart README's table of what the agent reads and sends is kept in step with it.

## Configuration

Environment variables only; the chart sets each from an `agent.*` value.

| Variable | Chart value | Default | Meaning |
|---|---|---|---|
| `COSTFLUENT_TOKEN` | `agent.token` or `agent.secret.*` | required | The cluster token Costfluent shows once when the cluster is added, checked at start. Read once at start, so a new token takes a restart. |
| `COSTFLUENT_CLUSTER_ID` | `agent.clusterID` | the token's cluster | When set, it must be the token's cluster: `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`. |
| `COSTFLUENT_API_ENDPOINT` | `agent.apiEndpoint` | `https://api.costfluent.com` | `http://` is accepted with a warning, for a local receiver. |
| `COSTFLUENT_POLLING_INTERVAL` | `agent.pollingInterval` | `60` | Seconds between polls: 5, 10, 15, 30 or 60. |
| `COSTFLUENT_NODE_ADDRESS_TYPES` | `agent.nodeAddressTypes` | `InternalIP,InternalDNS,Hostname,ExternalIP,ExternalDNS` | Node address types tried, in order, to reach each kubelet. |
| `COSTFLUENT_KUBE_SKIP_TLS_VERIFY` | `agent.disableKubeTLSverify` | `false` | Skip verifying kubelet serving certificates. |
| `COSTFLUENT_ALLOWED_LABELS` | `agent.allowedLabels` | empty: every pod label | Comma-separated pod label keys to send. |
| `COSTFLUENT_ALLOWED_ANNOTATIONS` | `agent.allowedAnnotations` | empty: none | Comma-separated pod annotation keys to send, at most 10; values are cut to 100 characters. |
| `COSTFLUENT_COLLECT_NAMESPACE_LABELS` | `agent.collectNamespaceLabels` | `false` | Send namespace labels. |
| `COSTFLUENT_COLLECT_PVC_LABELS` | `agent.collectPVCLabels` | `false` | Send each bound volume's claim labels; the agent then lists claims every five minutes. |
| `COSTFLUENT_KUBE_API_QPS`, `COSTFLUENT_KUBE_API_BURST` | `agent.kubeAPIQPS`, `agent.kubeAPIBurst` | `50`, `100` | Requests a second, and the burst, to the Kubernetes API server. The burst may not be lower than the rate. |
| `COSTFLUENT_SCRAPE_CONCURRENCY` | `agent.scrapeConcurrency` | `20` | The most kubelets scraped at once, 1 to 200. The agent scrapes one at a time per 20 nodes, at least 10, up to this. |
| `COSTFLUENT_OWNER_CACHE_SIZE` | `agent.ownerCacheSize` | `20000` | Resolved ReplicaSet and Job owners remembered, least recently used dropped first: 100 to 1,000,000. |
| `COSTFLUENT_REPORT_HTTP_PROXY` | `agent.reportHTTPProxy` | none | HTTP proxy for report traffic only. A value that is not an `http://` or `https://` URL with a host stops the start, with an error that does not show it. |
| `COSTFLUENT_REPORT_CA_FILE` | `agent.reportCABundle` | none: the system CAs | A PEM file whose certificates are trusted, beside the system CAs, for report and `/v1/me` traffic only, such as a TLS-inspecting proxy's CA. A file that is missing, cannot be read or holds no certificate stops the start. |
| `COSTFLUENT_KUBELET_SERVICE_ACCOUNT` | set by the chart | none: the agent's own token | The service account, in `COSTFLUENT_NAMESPACE`, whose token kubelet scrapes carry. Unset, as under a chart up to 0.3.0, the scrapes carry the agent's own service-account token and the agent warns at start. |
| `COSTFLUENT_NAMESPACE` | set by the chart from the pod's namespace | none | The agent's own namespace; required when `COSTFLUENT_KUBELET_SERVICE_ACCOUNT` is set. |
| `COSTFLUENT_DATA_DIR` | `persist.mountPath` | `/data` (the chart sets `/var/lib/costfluent`) | Buffer, window snapshot, instance ID and the token's cluster. Must be writable; the agent exits otherwise. |
| `COSTFLUENT_METRICS_PORT` | `service.port` | `9010` | Serves `/metrics`, `/healthz` and `/readyz`. |
| `COSTFLUENT_LOG_LEVEL` | `agent.logLevel` | `info` | `debug`, `info`, `warn` or `error`. |
| `COSTFLUENT_LOG_FORMAT` | `agent.extraEnv` | `json` | `json` or `console`. |
| `COSTFLUENT_MEMORY_LIMIT` | `resources.limits.memory`, passed by the chart | none | The container's memory limit in bytes. The agent keeps its Go heap under three quarters of it, unless `GOMEMLIMIT` is set. |

## Metrics and health

`/healthz` answers 503, so the kubelet restarts the agent, once no poll has succeeded for five
polling intervals and at least ten minutes; `/readyz` answers once the first poll has completed. `/metrics`
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
| `costfluent_agent_elapsed_seconds_total`, `costfluent_agent_credited_seconds_total` | | Seconds between successful polls, and the seconds of usage credited for them. Credited falls behind elapsed only when polls failed or were missed. |
| `costfluent_agent_kubelet_body_truncated_total` | | Kubelet `/metrics/resource` bodies cut at 32 MiB; the node is left out of that poll and counted as an `error` scrape. |
| `costfluent_agent_map_entries_dropped_total` | `map` (`node_labels`, `node_annotations`, `pod_labels`, `pod_annotations`, `namespace_labels`, `storage_class_annotations`, `claim_labels`) | Label and annotation entries left out to fit the API's caps, counted each time the map is read. |
| `costfluent_agent_volumes_dropped_total` | | Persistent volumes left out of a closed window past the API's 50,000; the first by name are sent. |
| `costfluent_agent_storage_unreadable` | | 1 while the last list of persistent volumes or storage classes failed, as when the ClusterRole lacks them; reports then carry no volumes, and nodes and pods report as before. |
| `costfluent_agent_over_supported_size` | | 1 when the last closed window was over the API's node, pod or container caps and was not sent. |

## Development

```bash
make check    # gofmt, go vet, golangci-lint, go test -race, build
make e2e      # all four legs below; each installs the chart with --wait and asserts what reaches a stub receiver over TLS
make e2e-kind         # kind; the receiver's CA through agent.reportCABundle, and the agent's own account cannot get nodes/metrics
make e2e-k3s          # k3s in a container, the console's self-managed choice; K3S_IMAGE and K3S_REGISTRY_MIRROR override it
make e2e-proxy        # reports through a credentialed CONNECT proxy whose URL comes from agent.reportHTTPProxySecret
make e2e-kubelet-tls  # kind with kubelet verification on: the first window has no nodes and unscraped_nodes.tls
make docker   # builds ghcr.io/costfluent/k8s-agent
```

The e2e targets need docker, kind, kubectl and helm. It reads the chart from `../helm-charts`, or from
`E2E_CHART`, and takes the kind node image from `KIND_NODE_IMAGE` when set. `E2E_KEEP_CLUSTER=1`
leaves the cluster running for inspection.

`make load` holds the agent to the memory limit the chart gives its pod, `resources.limits.memory`,
read from `../helm-charts/costfluent-k8s-agent/values.yaml`, or from `values.yaml` in the chart
directory `LOAD_CHART` names; a relative `LOAD_CHART` resolves from `test/load`.

The agent reads only the in-cluster configuration, so it runs as a pod, never from a workstation.

## License

Apache License 2.0; see [LICENSE](./LICENSE).
