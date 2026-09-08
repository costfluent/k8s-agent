package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all agent configuration
type Config struct {
	// Required
	Token       string `mapstructure:"token"`
	ClusterID   string `mapstructure:"cluster_id"`
	ClusterName string `mapstructure:"cluster_name"`

	// API endpoint
	APIEndpoint string `mapstructure:"api_endpoint"`

	// Intervals
	PollingInterval   time.Duration `mapstructure:"polling_interval"`
	ReportingInterval time.Duration `mapstructure:"reporting_interval"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`

	// Filtering
	NamespaceInclude []string `mapstructure:"namespace_include"`
	NamespaceExclude []string `mapstructure:"namespace_exclude"`

	// Metadata collection
	CollectAnnotations     bool     `mapstructure:"collect_annotations"`
	AllowedAnnotations     []string `mapstructure:"allowed_annotations"`
	MaxAnnotations         int      `mapstructure:"max_annotations"`
	MaxAnnotationLength    int      `mapstructure:"max_annotation_length"`
	CollectNamespaceLabels bool     `mapstructure:"collect_namespace_labels"`

	// Storage
	DataDir string `mapstructure:"data_dir"`

	// Metrics server
	MetricsEnabled bool `mapstructure:"metrics_enabled"`
	MetricsPort    int  `mapstructure:"metrics_port"`

	// Logging
	LogLevel  string `mapstructure:"log_level"`
	LogFormat string `mapstructure:"log_format"`
}

// Load reads configuration from environment and file
func Load() (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("api_endpoint", "https://api.costfluent.com")
	v.SetDefault("polling_interval", 60*time.Second)
	v.SetDefault("reporting_interval", 3600*time.Second)
	v.SetDefault("heartbeat_interval", 300*time.Second)
	v.SetDefault("namespace_exclude", []string{"kube-system", "kube-public"})
	// Opt-in: annotations often carry internal configuration, and this agent runs in someone
	// else's cluster. A customer who allocates cost by annotation names the prefixes they want
	// in allowed_annotations; nobody ships annotation values by accident.
	v.SetDefault("collect_annotations", false)
	v.SetDefault("max_annotations", 10)
	v.SetDefault("max_annotation_length", 100)
	v.SetDefault("collect_namespace_labels", true)
	v.SetDefault("data_dir", "/data")
	v.SetDefault("metrics_enabled", true)
	v.SetDefault("metrics_port", 9010)
	v.SetDefault("log_level", "info")
	v.SetDefault("log_format", "json")

	// Environment variables
	v.SetEnvPrefix("COSTFLUENT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// Config file (optional)
	configPath := os.Getenv("COSTFLUENT_CONFIG_PATH")
	if configPath == "" {
		configPath = "/etc/costfluent"
	}
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(configPath)
	v.AddConfigPath(".")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks required fields
func (c *Config) Validate() error {
	if c.Token == "" {
		return fmt.Errorf("token is required (set COSTFLUENT_TOKEN)")
	}
	if c.ClusterID == "" {
		return fmt.Errorf("cluster_id is required (set COSTFLUENT_CLUSTER_ID)")
	}
	if c.PollingInterval < 5*time.Second {
		return fmt.Errorf("polling_interval must be >= 5s")
	}
	if c.ReportingInterval < 60*time.Second {
		return fmt.Errorf("reporting_interval must be >= 60s")
	}
	return nil
}

// ShouldCollectNamespace returns true if namespace should be collected
func (c *Config) ShouldCollectNamespace(ns string) bool {
	// Check exclusions first
	for _, excluded := range c.NamespaceExclude {
		if excluded == ns {
			return false
		}
	}
	// If inclusions specified, namespace must be in list
	if len(c.NamespaceInclude) > 0 {
		for _, included := range c.NamespaceInclude {
			if included == ns {
				return true
			}
		}
		return false
	}
	return true
}
