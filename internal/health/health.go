package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Status represents health check results
type Status struct {
	Healthy     bool      `json:"healthy"`
	K8sReady    bool      `json:"k8s_ready"`
	APIReady    bool      `json:"api_ready"`
	LastScrape  time.Time `json:"last_scrape,omitempty"`
	LastReport  time.Time `json:"last_report,omitempty"`
	Message     string    `json:"message,omitempty"`
	CheckedAt   time.Time `json:"checked_at"`
}

// Checker performs health checks
type Checker struct {
	mu          sync.RWMutex
	k8sClient   kubernetes.Interface
	apiEndpoint string
	httpClient  *http.Client

	lastScrape time.Time
	lastReport time.Time
	k8sReady   bool
	apiReady   bool
}

// NewChecker creates a new health checker
func NewChecker(k8sClient kubernetes.Interface, apiEndpoint string) *Checker {
	return &Checker{
		k8sClient:   k8sClient,
		apiEndpoint: apiEndpoint,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// RecordScrape records successful scrape time
func (c *Checker) RecordScrape() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastScrape = time.Now()
}

// RecordReport records successful report time
func (c *Checker) RecordReport() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastReport = time.Now()
}

// CheckK8s verifies K8s API connectivity
func (c *Checker) CheckK8s(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.k8sClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	ready := err == nil

	c.mu.Lock()
	c.k8sReady = ready
	c.mu.Unlock()

	return ready
}

// CheckAPI verifies Costfluent API connectivity
func (c *Checker) CheckAPI(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiEndpoint+"/health", nil)
	if err != nil {
		c.mu.Lock()
		c.apiReady = false
		c.mu.Unlock()
		return false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.mu.Lock()
		c.apiReady = false
		c.mu.Unlock()
		return false
	}
	defer resp.Body.Close()

	ready := resp.StatusCode == http.StatusOK

	c.mu.Lock()
	c.apiReady = ready
	c.mu.Unlock()

	return ready
}

// GetStatus returns current health status
func (c *Checker) GetStatus() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Consider healthy if K8s is reachable and we've scraped recently
	scrapeOK := !c.lastScrape.IsZero() && time.Since(c.lastScrape) < 5*time.Minute
	healthy := c.k8sReady && scrapeOK

	var msg string
	if !c.k8sReady {
		msg = "kubernetes API unreachable"
	} else if !scrapeOK {
		msg = "no recent scrape"
	} else if !c.apiReady {
		msg = "costfluent API unreachable (buffering)"
	}

	return Status{
		Healthy:    healthy,
		K8sReady:   c.k8sReady,
		APIReady:   c.apiReady,
		LastScrape: c.lastScrape,
		LastReport: c.lastReport,
		Message:    msg,
		CheckedAt:  time.Now(),
	}
}

// LivenessHandler returns HTTP handler for /healthz
func (c *Checker) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Liveness: is the process alive and not deadlocked?
		// Check K8s connectivity as basic sanity
		ctx := r.Context()
		if !c.CheckK8s(ctx) {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("kubernetes API unreachable"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}
}

// ReadinessHandler returns HTTP handler for /readyz
func (c *Checker) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := c.GetStatus()

		w.Header().Set("Content-Type", "application/json")

		if !status.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		json.NewEncoder(w).Encode(status)
	}
}
