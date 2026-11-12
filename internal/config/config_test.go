package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// inChart is what the chart always sets; a test that leaves one out of its values gets it from here,
// and one that sets it, even to "", overrides it.
var inChart = map[string]string{
	"COSTFLUENT_KUBELET_SERVICE_ACCOUNT": "costfluent-k8s-agent-kubelet",
	"COSTFLUENT_NAMESPACE":               "costfluent",
}

func env(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		if v, ok := values[key]; ok {
			return v, true
		}
		v, ok := inChart[key]
		return v, ok
	}
}

func TestLoad_ReadsRequiredValuesFromEnvironment(t *testing.T) {
	t.Setenv("COSTFLUENT_TOKEN", "cf_org_secret")
	t.Setenv("COSTFLUENT_CLUSTER_ID", "pve-dev")
	t.Setenv("COSTFLUENT_KUBELET_SERVICE_ACCOUNT", "costfluent-k8s-agent-kubelet")
	t.Setenv("COSTFLUENT_NAMESPACE", "costfluent")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Token != "cf_org_secret" || cfg.ClusterID != "pve-dev" {
		t.Fatalf("got token %q cluster %q", cfg.Token, cfg.ClusterID)
	}
	if cfg.APIEndpoint.String() != DefaultAPIEndpoint {
		t.Errorf("endpoint = %s", cfg.APIEndpoint)
	}
	if cfg.PollingInterval != 60*time.Second || cfg.DataDir != "/data" || cfg.MetricsPort != 9010 {
		t.Errorf("defaults = %v %q %d", cfg.PollingInterval, cfg.DataDir, cfg.MetricsPort)
	}
	if cfg.CollectNamespaceLabels || cfg.KubeSkipTLSVerify || len(cfg.AllowedAnnotations) != 0 || len(cfg.AllowedLabels) != 0 {
		t.Errorf("collection defaults are not the minimum: %+v", cfg)
	}
	if len(cfg.Warnings) != 0 {
		t.Errorf("warnings = %v", cfg.Warnings)
	}
	if cfg.KubeletServiceAccount != "costfluent-k8s-agent-kubelet" || cfg.Namespace != "costfluent" {
		t.Errorf("kubelet account = %q in %q", cfg.KubeletServiceAccount, cfg.Namespace)
	}
	if cfg.ReportRootCAs != nil {
		t.Errorf("report CAs = %v, want nil so the system pool applies", cfg.ReportRootCAs)
	}
}

func TestLoad_ReadsOptionalValues(t *testing.T) {
	cfg, err := load(env(map[string]string{
		"COSTFLUENT_TOKEN":                    "t",
		"COSTFLUENT_CLUSTER_ID":               "a.b_c-1",
		"COSTFLUENT_API_ENDPOINT":             "http://receiver.e2e:8080/",
		"COSTFLUENT_POLLING_INTERVAL":         "5",
		"COSTFLUENT_NODE_ADDRESS_TYPES":       "Hostname, ExternalIP",
		"COSTFLUENT_KUBE_SKIP_TLS_VERIFY":     "true",
		"COSTFLUENT_ALLOWED_LABELS":           "app,team",
		"COSTFLUENT_ALLOWED_ANNOTATIONS":      "owner",
		"COSTFLUENT_COLLECT_NAMESPACE_LABELS": "true",
		"COSTFLUENT_REPORT_HTTP_PROXY":        "http://proxy:3128",
		"COSTFLUENT_DATA_DIR":                 "/var/lib/cf",
		"COSTFLUENT_METRICS_PORT":             "9100",
		"COSTFLUENT_LOG_LEVEL":                "debug",
		"COSTFLUENT_LOG_FORMAT":               "console",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.APIEndpoint.String() != "http://receiver.e2e:8080" {
		t.Errorf("endpoint = %s", cfg.APIEndpoint)
	}
	if len(cfg.Warnings) != 1 || !strings.Contains(cfg.Warnings[0], "plain http") {
		t.Errorf("warnings = %v", cfg.Warnings)
	}
	if cfg.PollingInterval != 5*time.Second {
		t.Errorf("interval = %v", cfg.PollingInterval)
	}
	if len(cfg.NodeAddressTypes) != 2 || cfg.NodeAddressTypes[0] != corev1.NodeHostName || cfg.NodeAddressTypes[1] != corev1.NodeExternalIP {
		t.Errorf("address types = %v", cfg.NodeAddressTypes)
	}
	if !cfg.KubeSkipTLSVerify || !cfg.CollectNamespaceLabels {
		t.Errorf("bools = %+v", cfg)
	}
	if strings.Join(cfg.AllowedLabels, ",") != "app,team" || strings.Join(cfg.AllowedAnnotations, ",") != "owner" {
		t.Errorf("allowlists = %v %v", cfg.AllowedLabels, cfg.AllowedAnnotations)
	}
	if cfg.ReportHTTPProxy.String() != "http://proxy:3128" || cfg.DataDir != "/var/lib/cf" || cfg.MetricsPort != 9100 {
		t.Errorf("proxy/dir/port = %v %q %d", cfg.ReportHTTPProxy, cfg.DataDir, cfg.MetricsPort)
	}
}

func TestLoad_RejectsInvalidValues(t *testing.T) {
	base := map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_CLUSTER_ID": "c"}
	cases := map[string]map[string]string{
		"missing token":            {"COSTFLUENT_TOKEN": ""},
		"cluster leading dot":      {"COSTFLUENT_CLUSTER_ID": ".abc"},
		"cluster too long":         {"COSTFLUENT_CLUSTER_ID": strings.Repeat("a", 64)},
		"cluster slash":            {"COSTFLUENT_CLUSTER_ID": "a/b"},
		"endpoint scheme":          {"COSTFLUENT_API_ENDPOINT": "ftp://x"},
		"endpoint relative":        {"COSTFLUENT_API_ENDPOINT": "api.costfluent.com"},
		"interval not allowed":     {"COSTFLUENT_POLLING_INTERVAL": "20"},
		"interval with unit":       {"COSTFLUENT_POLLING_INTERVAL": "60s"},
		"address type":             {"COSTFLUENT_NODE_ADDRESS_TYPES": "InternalIP,Bogus"},
		"skip verify":              {"COSTFLUENT_KUBE_SKIP_TLS_VERIFY": "maybe"},
		"eleven annotations":       {"COSTFLUENT_ALLOWED_ANNOTATIONS": "a,b,c,d,e,f,g,h,i,j,k"},
		"proxy":                    {"COSTFLUENT_REPORT_HTTP_PROXY": "proxy:3128"},
		"metrics port":             {"COSTFLUENT_METRICS_PORT": "70000"},
		"log level":                {"COSTFLUENT_LOG_LEVEL": "trace"},
		"log format":               {"COSTFLUENT_LOG_FORMAT": "text"},
		"namespace labels boolean": {"COSTFLUENT_COLLECT_NAMESPACE_LABELS": "yes please"},
		"kubelet account name":     {"COSTFLUENT_KUBELET_SERVICE_ACCOUNT": "Not_A_Name"},
		"namespace name":           {"COSTFLUENT_NAMESPACE": "a.b"},
	}
	for name, override := range cases {
		t.Run(name, func(t *testing.T) {
			values := map[string]string{}
			for k, v := range base {
				values[k] = v
			}
			for k, v := range override {
				values[k] = v
			}
			if _, err := load(env(values)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// The cluster token names its cluster, so the chart may leave agent.clusterID out.
func TestLoad_ClusterIDIsOptional(t *testing.T) {
	cfg, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t"}))
	if err != nil || cfg.ClusterID != "" {
		t.Fatalf("got cluster %q, err %v; want no cluster and no error", cfg.ClusterID, err)
	}
}

func TestLoad_AcceptsLongestClusterID(t *testing.T) {
	id := strings.Repeat("a", 63)
	cfg, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_CLUSTER_ID": id}))
	if err != nil || cfg.ClusterID != id {
		t.Fatalf("got %v, %v", cfg, err)
	}
}

func TestLoad_ReadsTheContainerMemoryLimit(t *testing.T) {
	base := map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_CLUSTER_ID": "c"}
	cfg, err := load(env(base))
	if err != nil || cfg.MemoryLimitBytes != 0 || cfg.GoMemoryLimit() != 0 {
		t.Fatalf("unset limit = %d, soft %d, %v; want none", cfg.MemoryLimitBytes, cfg.GoMemoryLimit(), err)
	}
	base["COSTFLUENT_MEMORY_LIMIT"] = "268435456"
	cfg, err = load(env(base))
	if err != nil || cfg.MemoryLimitBytes != 256<<20 || cfg.GoMemoryLimit() != 192<<20 {
		t.Fatalf("limit = %d, soft %d, %v; want 256 MiB and a 192 MiB soft limit", cfg.MemoryLimitBytes, cfg.GoMemoryLimit(), err)
	}
	for _, bad := range []string{"256Mi", "0", "-1", "lots"} {
		base["COSTFLUENT_MEMORY_LIMIT"] = bad
		if _, err := load(env(base)); err == nil || !strings.Contains(err.Error(), "COSTFLUENT_MEMORY_LIMIT") {
			t.Errorf("COSTFLUENT_MEMORY_LIMIT=%q: err = %v, want it refused", bad, err)
		}
	}
}

func TestLoad_ReadsTheAPIRateAndOwnerCacheSize(t *testing.T) {
	base := map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_CLUSTER_ID": "c"}
	cfg, err := load(env(base))
	if err != nil || cfg.KubeAPIQPS != DefaultKubeAPIQPS || cfg.KubeAPIBurst != DefaultKubeAPIBurst || cfg.OwnerCacheSize != DefaultOwnerCacheSize {
		t.Fatalf("defaults = %d %d %d, %v", cfg.KubeAPIQPS, cfg.KubeAPIBurst, cfg.OwnerCacheSize, err)
	}
	set := map[string]string{"COSTFLUENT_KUBE_API_QPS": "20", "COSTFLUENT_KUBE_API_BURST": "40", "COSTFLUENT_OWNER_CACHE_SIZE": "5000"}
	for k, v := range set {
		base[k] = v
	}
	cfg, err = load(env(base))
	if err != nil || cfg.KubeAPIQPS != 20 || cfg.KubeAPIBurst != 40 || cfg.OwnerCacheSize != 5000 {
		t.Fatalf("set = %d %d %d, %v", cfg.KubeAPIQPS, cfg.KubeAPIBurst, cfg.OwnerCacheSize, err)
	}
	for name, override := range map[string]map[string]string{
		"qps zero":           {"COSTFLUENT_KUBE_API_QPS": "0"},
		"qps too high":       {"COSTFLUENT_KUBE_API_QPS": "1001"},
		"qps not a number":   {"COSTFLUENT_KUBE_API_QPS": "fast"},
		"burst below qps":    {"COSTFLUENT_KUBE_API_BURST": "10"},
		"burst not a number": {"COSTFLUENT_KUBE_API_BURST": "x"},
		"cache too small":    {"COSTFLUENT_OWNER_CACHE_SIZE": "99"},
		"cache too large":    {"COSTFLUENT_OWNER_CACHE_SIZE": "1000001"},
		"cache not a number": {"COSTFLUENT_OWNER_CACHE_SIZE": "lots"},
	} {
		values := map[string]string{}
		for k, v := range base {
			values[k] = v
		}
		for k, v := range override {
			values[k] = v
		}
		if _, err := load(env(values)); err == nil {
			t.Errorf("%s: accepted %v", name, override)
		}
	}
}

func TestLoad_ReadsTheScrapeConcurrency(t *testing.T) {
	base := map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_CLUSTER_ID": "c"}
	if cfg, err := load(env(base)); err != nil || cfg.ScrapeConcurrency != 20 {
		t.Fatalf("default = %v, %v; want 20", cfg, err)
	}
	base["COSTFLUENT_SCRAPE_CONCURRENCY"] = "50"
	if cfg, err := load(env(base)); err != nil || cfg.ScrapeConcurrency != 50 {
		t.Fatalf("set = %v, %v; want 50", cfg, err)
	}
	for _, bad := range []string{"0", "201", "many"} {
		base["COSTFLUENT_SCRAPE_CONCURRENCY"] = bad
		if _, err := load(env(base)); err == nil {
			t.Errorf("COSTFLUENT_SCRAPE_CONCURRENCY=%q accepted", bad)
		}
	}
}

// Claim labels are a tenant's to set, so the agent lists claims only when the owner opts in.
func TestParse_CollectPVCLabels(t *testing.T) {
	for raw, want := range map[string]bool{"": false, "false": false, "true": true, "1": true} {
		t.Run(raw, func(t *testing.T) {
			cfg, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_COLLECT_PVC_LABELS": raw}))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.CollectPVCLabels != want {
				t.Errorf("COSTFLUENT_COLLECT_PVC_LABELS=%q gives %v, want %v", raw, cfg.CollectPVCLabels, want)
			}
		})
	}
	if _, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_COLLECT_PVC_LABELS": "yes please"})); err == nil ||
		!strings.Contains(err.Error(), "COSTFLUENT_COLLECT_PVC_LABELS") {
		t.Fatalf("err = %v, want one naming COSTFLUENT_COLLECT_PVC_LABELS", err)
	}
}

// caPEM is a self-signed CA certificate in PEM, standing in for a TLS-inspecting proxy's CA.
func caPEM(t *testing.T) (*x509.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Corporate Inspection CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_ReadsTheReportCAFileIntoTheSystemPool(t *testing.T) {
	ca, pemBytes := caPEM(t)
	cfg, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_REPORT_CA_FILE": writeFile(t, pemBytes)}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReportRootCAs == nil {
		t.Fatal("report CAs = nil, want the system pool plus the file's certificate")
	}
	if _, err := ca.Verify(x509.VerifyOptions{Roots: cfg.ReportRootCAs}); err != nil {
		t.Errorf("the file's CA does not verify against the pool: %v", err)
	}
	system, err := x509.SystemCertPool()
	if err != nil {
		t.Fatal(err)
	}
	system.AddCert(ca)
	if !cfg.ReportRootCAs.Equal(system) {
		t.Error("the pool is not the system pool plus the file's certificate")
	}
}

func TestLoad_RefusesAMissingOrUnreadableReportCAFile(t *testing.T) {
	for name, path := range map[string]string{
		"missing":    filepath.Join(t.TempDir(), "missing.crt"),
		"unreadable": t.TempDir(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_REPORT_CA_FILE": path}))
			if err == nil || !strings.Contains(err.Error(), "COSTFLUENT_REPORT_CA_FILE") || !strings.Contains(err.Error(), path) {
				t.Fatalf("err = %v, want one naming COSTFLUENT_REPORT_CA_FILE and %s", err, path)
			}
		})
	}
}

func TestLoad_RefusesAReportCAFileWithoutCertificates(t *testing.T) {
	const content = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0LWtleS1tYXRlcmlhbA==\n-----END PRIVATE KEY-----\n"
	path := writeFile(t, []byte(content))
	_, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_REPORT_CA_FILE": path}))
	if err == nil || !strings.Contains(err.Error(), "COSTFLUENT_REPORT_CA_FILE") || !strings.Contains(err.Error(), path) {
		t.Fatalf("err = %v, want one naming COSTFLUENT_REPORT_CA_FILE and %s", err, path)
	}
	if strings.Contains(err.Error(), "c2VjcmV0") || strings.Contains(err.Error(), "PRIVATE KEY") {
		t.Errorf("err = %v, which shows the file's content", err)
	}
}

// An agent newer than its chart finds no metrics account and keeps the old chart's way of scraping.
func TestLoad_WithoutTheKubeletAccountWarnsAndScrapesWithTheAgentsOwnToken(t *testing.T) {
	cfg, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_KUBELET_SERVICE_ACCOUNT": "", "COSTFLUENT_NAMESPACE": ""}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KubeletServiceAccount != "" {
		t.Errorf("kubelet account = %q, want none", cfg.KubeletServiceAccount)
	}
	if len(cfg.Warnings) != 1 || !strings.Contains(cfg.Warnings[0], "COSTFLUENT_KUBELET_SERVICE_ACCOUNT") {
		t.Errorf("warnings = %v, want one naming COSTFLUENT_KUBELET_SERVICE_ACCOUNT", cfg.Warnings)
	}
}

func TestLoad_RefusesAKubeletAccountWithoutANamespace(t *testing.T) {
	_, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_NAMESPACE": ""}))
	if err == nil || !strings.Contains(err.Error(), "COSTFLUENT_NAMESPACE") {
		t.Fatalf("err = %v, want one naming COSTFLUENT_NAMESPACE", err)
	}
}

func TestLoad_ReportsASystemPoolItCannotLoad(t *testing.T) {
	original := systemCertPool
	t.Cleanup(func() { systemCertPool = original })
	systemCertPool = func() (*x509.CertPool, error) { return nil, errors.New("no system roots") }
	_, pemBytes := caPEM(t)
	path := writeFile(t, pemBytes)
	_, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_REPORT_CA_FILE": path}))
	if err == nil || !strings.Contains(err.Error(), "COSTFLUENT_REPORT_CA_FILE") || !strings.Contains(err.Error(), "no system roots") {
		t.Fatalf("err = %v, want one naming COSTFLUENT_REPORT_CA_FILE and the cause", err)
	}
}

// A proxy URL can carry credentials, and without a scheme url.Parse reads them as the host's
// password, so the error names the variable and never echoes the value. A value url.Parse rejects
// outright (an unclosed IPv6 host) is in the list too, because url.Parse's own error quotes the
// whole value.
func TestLoad_ProxyErrorNeverShowsTheValue(t *testing.T) {
	for _, raw := range []string{"user:s3cr3t-pass@proxy:3128", "ftp://user:s3cr3t-pass@proxy:3128", "http://user:s3cr3t-pass@", "http://user:s3cr3t-pass@[::1"} {
		t.Run(raw, func(t *testing.T) {
			_, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_REPORT_HTTP_PROXY": raw}))
			if err == nil || !strings.Contains(err.Error(), "COSTFLUENT_REPORT_HTTP_PROXY") {
				t.Fatalf("err = %v, want one naming COSTFLUENT_REPORT_HTTP_PROXY", err)
			}
			if strings.Contains(err.Error(), "s3cr3t") || strings.Contains(err.Error(), "user") {
				t.Errorf("err = %v, which shows the proxy's credentials", err)
			}
		})
	}
}
