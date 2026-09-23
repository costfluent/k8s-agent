package config

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func env(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

func TestLoad_ReadsRequiredValuesFromEnvironment(t *testing.T) {
	t.Setenv("COSTFLUENT_TOKEN", "cf_org_secret")
	t.Setenv("COSTFLUENT_CLUSTER_ID", "pve-dev")

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
		"missing cluster":          {"COSTFLUENT_CLUSTER_ID": ""},
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

func TestLoad_AcceptsLongestClusterID(t *testing.T) {
	id := strings.Repeat("a", 63)
	cfg, err := load(env(map[string]string{"COSTFLUENT_TOKEN": "t", "COSTFLUENT_CLUSTER_ID": id}))
	if err != nil || cfg.ClusterID != id {
		t.Fatalf("got %v, %v", cfg, err)
	}
}
