// Package config reads the agent's configuration from COSTFLUENT_* environment variables.
package config

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	DefaultAPIEndpoint       = "https://api.costfluent.com"
	DefaultDataDir           = "/data"
	DefaultMetricsPort       = 9010
	DefaultPollingInterval   = 60 * time.Second
	MaxAllowedAnnotations    = 10
	MaxAnnotationValueChars  = 100
	DefaultKubeAPIQPS        = 50
	DefaultKubeAPIBurst      = 100
	DefaultOwnerCacheSize    = 20000
	DefaultScrapeConcurrency = 20
	maxScrapeConcurrency     = 200
	maxKubeAPIQPS            = 1000
	minOwnerCacheSize        = 100
	maxOwnerCacheSize        = 1000000
)

// DefaultNodeAddressTypes is the order a node's addresses are tried in to reach its kubelet.
var DefaultNodeAddressTypes = []corev1.NodeAddressType{
	corev1.NodeInternalIP,
	corev1.NodeInternalDNS,
	corev1.NodeHostName,
	corev1.NodeExternalIP,
	corev1.NodeExternalDNS,
}

var (
	clusterIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
	pollingIntervals  = []int{5, 10, 15, 30, 60}
	knownAddressTypes = map[string]corev1.NodeAddressType{
		string(corev1.NodeHostName):    corev1.NodeHostName,
		string(corev1.NodeInternalIP):  corev1.NodeInternalIP,
		string(corev1.NodeInternalDNS): corev1.NodeInternalDNS,
		string(corev1.NodeExternalIP):  corev1.NodeExternalIP,
		string(corev1.NodeExternalDNS): corev1.NodeExternalDNS,
	}
)

// Config is the complete agent configuration.
type Config struct {
	Token                  string
	ClusterID              string
	APIEndpoint            *url.URL
	PollingInterval        time.Duration
	NodeAddressTypes       []corev1.NodeAddressType
	KubeSkipTLSVerify      bool
	AllowedLabels          []string
	AllowedAnnotations     []string
	CollectNamespaceLabels bool
	// CollectPVCLabels sends each bound volume's claim labels, which takes listing every claim.
	CollectPVCLabels bool
	ReportHTTPProxy  *url.URL
	// ReportRootCAs is the system pool plus COSTFLUENT_REPORT_CA_FILE's certificates, trusted for
	// report and /v1/me traffic only; nil when the file is not set, so the system pool applies.
	ReportRootCAs *x509.CertPool
	// KubeletServiceAccount names the service account, in Namespace, whose token the agent requests
	// for its kubelet scrapes, so kubelets never see the agent's own token. Empty under a chart that
	// does not create it: the scrapes then carry the agent's own token.
	KubeletServiceAccount string
	// Namespace is the agent's own namespace; set whenever KubeletServiceAccount is.
	Namespace   string
	DataDir     string
	MetricsPort int
	// KubeAPIQPS and KubeAPIBurst bound the agent's requests to the Kubernetes API server.
	KubeAPIQPS   int
	KubeAPIBurst int
	// ScrapeConcurrency is the most kubelets scraped at once.
	ScrapeConcurrency int
	// OwnerCacheSize is how many resolved ReplicaSet and Job owners the agent remembers.
	OwnerCacheSize int
	// MemoryLimitBytes is the container's memory limit, which the chart passes in. Zero when unset.
	MemoryLimitBytes int64
	LogLevel         string
	LogFormat        string

	// Warnings are non-fatal findings the caller logs once a logger exists.
	Warnings []string
}

// GoMemoryLimit is the soft limit the Go runtime keeps the agent's memory under: three quarters of
// the container's limit, leaving the rest for what the runtime holds outside the heap. Zero means
// no limit.
func (c *Config) GoMemoryLimit() int64 {
	return c.MemoryLimitBytes / 4 * 3
}

// Load reads the configuration from the process environment.
func Load() (*Config, error) {
	return load(os.LookupEnv)
}

func load(lookup func(string) (string, bool)) (*Config, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}

	var errs []error
	cfg := &Config{
		Token:     get("COSTFLUENT_TOKEN"),
		ClusterID: get("COSTFLUENT_CLUSTER_ID"),
		DataDir:   orDefault(get("COSTFLUENT_DATA_DIR"), DefaultDataDir),
		LogLevel:  orDefault(get("COSTFLUENT_LOG_LEVEL"), "info"),
		LogFormat: orDefault(get("COSTFLUENT_LOG_FORMAT"), "json"),
	}

	if cfg.Token == "" {
		errs = append(errs, errors.New("COSTFLUENT_TOKEN is required: set agent.token or agent.secret.name in the chart"))
	}
	// Optional: the cluster token names its cluster, and the agent reads it from GET /v1/me at
	// start. When set, it has to be that cluster's id.
	if cfg.ClusterID != "" && !clusterIDPattern.MatchString(cfg.ClusterID) {
		errs = append(errs, fmt.Errorf("COSTFLUENT_CLUSTER_ID %q must match %s", cfg.ClusterID, clusterIDPattern))
	}

	endpoint, warning, err := parseEndpoint(orDefault(get("COSTFLUENT_API_ENDPOINT"), DefaultAPIEndpoint))
	if err != nil {
		errs = append(errs, err)
	}
	cfg.APIEndpoint = endpoint
	if warning != "" {
		cfg.Warnings = append(cfg.Warnings, warning)
	}

	cfg.PollingInterval = DefaultPollingInterval
	if raw := get("COSTFLUENT_POLLING_INTERVAL"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || !contains(pollingIntervals, seconds) {
			errs = append(errs, fmt.Errorf("COSTFLUENT_POLLING_INTERVAL %q must be one of 5, 10, 15, 30 or 60 seconds", raw))
		} else {
			cfg.PollingInterval = time.Duration(seconds) * time.Second
		}
	}

	cfg.NodeAddressTypes = DefaultNodeAddressTypes
	if raw := get("COSTFLUENT_NODE_ADDRESS_TYPES"); raw != "" {
		types, err := parseAddressTypes(raw)
		if err != nil {
			errs = append(errs, err)
		} else {
			cfg.NodeAddressTypes = types
		}
	}

	if cfg.KubeSkipTLSVerify, err = parseBool("COSTFLUENT_KUBE_SKIP_TLS_VERIFY", get("COSTFLUENT_KUBE_SKIP_TLS_VERIFY")); err != nil {
		errs = append(errs, err)
	}
	if cfg.CollectNamespaceLabels, err = parseBool("COSTFLUENT_COLLECT_NAMESPACE_LABELS", get("COSTFLUENT_COLLECT_NAMESPACE_LABELS")); err != nil {
		errs = append(errs, err)
	}
	if cfg.CollectPVCLabels, err = parseBool("COSTFLUENT_COLLECT_PVC_LABELS", get("COSTFLUENT_COLLECT_PVC_LABELS")); err != nil {
		errs = append(errs, err)
	}

	cfg.AllowedLabels = splitList(get("COSTFLUENT_ALLOWED_LABELS"))
	cfg.AllowedAnnotations = splitList(get("COSTFLUENT_ALLOWED_ANNOTATIONS"))
	if len(cfg.AllowedAnnotations) > MaxAllowedAnnotations {
		errs = append(errs, fmt.Errorf("COSTFLUENT_ALLOWED_ANNOTATIONS names %d keys; at most %d are allowed", len(cfg.AllowedAnnotations), MaxAllowedAnnotations))
	}

	if raw := get("COSTFLUENT_REPORT_HTTP_PROXY"); raw != "" {
		proxy, err := url.Parse(raw)
		if err != nil || proxy.Host == "" || (proxy.Scheme != "http" && proxy.Scheme != "https") {
			// The value may carry the proxy's credentials, which a value without a scheme parses as
			// a password, so neither it nor the parse error that quotes it is shown.
			errs = append(errs, errors.New("COSTFLUENT_REPORT_HTTP_PROXY must be an http:// or https:// URL with a host; its value is not shown, as it may hold credentials"))
		} else {
			cfg.ReportHTTPProxy = proxy
		}
	}
	if path := get("COSTFLUENT_REPORT_CA_FILE"); path != "" {
		pool, err := reportRootCAs(path)
		if err != nil {
			errs = append(errs, err)
		} else {
			cfg.ReportRootCAs = pool
		}
	}

	// A chart up to 0.3.0 sets neither, and still grants the agent's own account nodes/metrics, so a
	// newer agent under it keeps scraping with that account's token.
	cfg.KubeletServiceAccount = get("COSTFLUENT_KUBELET_SERVICE_ACCOUNT")
	cfg.Namespace = get("COSTFLUENT_NAMESPACE")
	switch {
	case cfg.KubeletServiceAccount == "":
		cfg.Warnings = append(cfg.Warnings, "COSTFLUENT_KUBELET_SERVICE_ACCOUNT is not set, so kubelet scrapes carry the agent's own service-account token, which reads every pod; upgrade the chart so they carry a token that reads only node metrics")
	case cfg.Namespace == "":
		errs = append(errs, errors.New("COSTFLUENT_NAMESPACE must name the agent's namespace when COSTFLUENT_KUBELET_SERVICE_ACCOUNT is set; the chart sets both"))
	default:
		if err := checkName("COSTFLUENT_KUBELET_SERVICE_ACCOUNT", cfg.KubeletServiceAccount, validation.IsDNS1123Subdomain); err != nil {
			errs = append(errs, err)
		}
		if err := checkName("COSTFLUENT_NAMESPACE", cfg.Namespace, validation.IsDNS1123Label); err != nil {
			errs = append(errs, err)
		}
	}

	cfg.MetricsPort = DefaultMetricsPort
	if raw := get("COSTFLUENT_METRICS_PORT"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			errs = append(errs, fmt.Errorf("COSTFLUENT_METRICS_PORT %q must be a port number", raw))
		} else {
			cfg.MetricsPort = port
		}
	}

	cfg.KubeAPIQPS = DefaultKubeAPIQPS
	if raw := get("COSTFLUENT_KUBE_API_QPS"); raw != "" {
		qps, err := strconv.Atoi(raw)
		if err != nil || qps < 1 || qps > maxKubeAPIQPS {
			errs = append(errs, fmt.Errorf("COSTFLUENT_KUBE_API_QPS %q must be a whole number from 1 to %d", raw, maxKubeAPIQPS))
		} else {
			cfg.KubeAPIQPS = qps
		}
	}
	cfg.KubeAPIBurst = max(DefaultKubeAPIBurst, cfg.KubeAPIQPS)
	if raw := get("COSTFLUENT_KUBE_API_BURST"); raw != "" {
		burst, err := strconv.Atoi(raw)
		if err != nil || burst < cfg.KubeAPIQPS {
			errs = append(errs, fmt.Errorf("COSTFLUENT_KUBE_API_BURST %q must be a whole number no lower than COSTFLUENT_KUBE_API_QPS (%d)", raw, cfg.KubeAPIQPS))
		} else {
			cfg.KubeAPIBurst = burst
		}
	}
	cfg.ScrapeConcurrency = DefaultScrapeConcurrency
	if raw := get("COSTFLUENT_SCRAPE_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxScrapeConcurrency {
			errs = append(errs, fmt.Errorf("COSTFLUENT_SCRAPE_CONCURRENCY %q must be a whole number from 1 to %d", raw, maxScrapeConcurrency))
		} else {
			cfg.ScrapeConcurrency = n
		}
	}
	cfg.OwnerCacheSize = DefaultOwnerCacheSize
	if raw := get("COSTFLUENT_OWNER_CACHE_SIZE"); raw != "" {
		size, err := strconv.Atoi(raw)
		if err != nil || size < minOwnerCacheSize || size > maxOwnerCacheSize {
			errs = append(errs, fmt.Errorf("COSTFLUENT_OWNER_CACHE_SIZE %q must be a whole number from %d to %d", raw, minOwnerCacheSize, maxOwnerCacheSize))
		} else {
			cfg.OwnerCacheSize = size
		}
	}

	if raw := get("COSTFLUENT_MEMORY_LIMIT"); raw != "" {
		limit, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || limit <= 0 {
			errs = append(errs, fmt.Errorf("COSTFLUENT_MEMORY_LIMIT %q must be a number of bytes", raw))
		} else {
			cfg.MemoryLimitBytes = limit
		}
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("COSTFLUENT_LOG_LEVEL %q must be debug, info, warn or error", cfg.LogLevel))
	}
	switch cfg.LogFormat {
	case "json", "console":
	default:
		errs = append(errs, fmt.Errorf("COSTFLUENT_LOG_FORMAT %q must be json or console", cfg.LogFormat))
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseEndpoint(raw string) (*url.URL, string, error) {
	u, err := url.Parse(strings.TrimRight(raw, "/"))
	if err != nil || u.Host == "" {
		return nil, "", fmt.Errorf("COSTFLUENT_API_ENDPOINT %q must be an absolute URL", raw)
	}
	switch u.Scheme {
	case "https":
		return u, "", nil
	case "http":
		return u, fmt.Sprintf("COSTFLUENT_API_ENDPOINT %s is plain http: the token and reports are sent unencrypted", u), nil
	default:
		return nil, "", fmt.Errorf("COSTFLUENT_API_ENDPOINT %q must use https:// (or http:// for a local receiver)", raw)
	}
}

func parseAddressTypes(raw string) ([]corev1.NodeAddressType, error) {
	var types []corev1.NodeAddressType
	for _, name := range splitList(raw) {
		t, ok := knownAddressTypes[name]
		if !ok {
			return nil, fmt.Errorf("COSTFLUENT_NODE_ADDRESS_TYPES: unknown address type %q (use Hostname, InternalDNS, InternalIP, ExternalDNS, ExternalIP)", name)
		}
		types = append(types, t)
	}
	if len(types) == 0 {
		return nil, errors.New("COSTFLUENT_NODE_ADDRESS_TYPES names no address type")
	}
	return types, nil
}

func parseBool(key, raw string) (bool, error) {
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s %q must be true or false", key, raw)
	}
	return v, nil
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func contains(values []int, v int) bool {
	for _, candidate := range values {
		if candidate == v {
			return true
		}
	}
	return false
}

// systemCertPool is x509.SystemCertPool, replaced in tests.
var systemCertPool = x509.SystemCertPool

// reportRootCAs is the system pool plus the certificates in the PEM file at path. Its errors name
// the file and never its content, which may be a key mounted by mistake.
func reportRootCAs(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the path is the operator's own setting
	if err != nil {
		return nil, fmt.Errorf("COSTFLUENT_REPORT_CA_FILE %s cannot be read: %w", path, err)
	}
	pool, err := systemCertPool()
	if err != nil {
		return nil, fmt.Errorf("COSTFLUENT_REPORT_CA_FILE %s: loading the system certificates to add it to: %w", path, err)
	}
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("COSTFLUENT_REPORT_CA_FILE %s holds no PEM certificate", path)
	}
	return pool, nil
}

// checkName holds a variable the chart sets to a Kubernetes object name.
func checkName(key, value string, valid func(string) []string) error {
	if problems := valid(value); len(problems) > 0 {
		return fmt.Errorf("%s %q is not a valid name: %s", key, value, strings.Join(problems, "; "))
	}
	return nil
}
