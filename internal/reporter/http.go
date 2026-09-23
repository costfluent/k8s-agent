// Package reporter posts buffered reports to POST {endpoint}/v1/kubernetes/reports.
package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

const (
	reportsPath    = "/v1/kubernetes/reports"
	requestTimeout = 30 * time.Second
	maxErrorBody   = 64 << 10
)

// Outcome is what the sender does with a report after one attempt.
type Outcome int

const (
	// Accepted: the API stored the report; remove it.
	Accepted Outcome = iota
	// Rejected: the API refused this report for good (400, 409, 410); remove it.
	Rejected
	// Unauthorized: the token is wrong or lacks the capability (401, 403). Keep the report and stop
	// sending until the agent restarts with a fixed token.
	Unauthorized
	// Retry: a transient failure (network, 413, 429, 5xx); keep the report and back off.
	Retry
)

// Result is one attempt's outcome and what the API said.
type Result struct {
	Outcome Outcome
	Status  int
	Message string
	ID      string
}

// Client sends reports.
type Client struct {
	url       string
	token     string
	userAgent string
	http      *http.Client
}

// NewClient builds a client for endpoint. proxy, when set, carries only report traffic; otherwise
// the standard proxy environment variables apply.
func NewClient(endpoint *url.URL, token, version string, proxy *url.URL) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxy != nil {
		transport.Proxy = http.ProxyURL(proxy)
	}
	return &Client{
		url:       strings.TrimRight(endpoint.String(), "/") + reportsPath,
		token:     token,
		userAgent: "costfluent-k8s-agent/" + version,
		http:      &http.Client{Transport: transport, Timeout: requestTimeout},
	}
}

// Send posts one gzip report body.
func (c *Client) Send(ctx context.Context, gzipBody []byte) Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(gzipBody))
	if err != nil {
		return Result{Outcome: Retry, Message: fmt.Sprintf("building request: %v", err)}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return Result{Outcome: Retry, Message: fmt.Sprintf("POST %s: %v", c.url, err)}
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))

	res := Result{Status: resp.StatusCode}
	switch s := resp.StatusCode; {
	case s >= 200 && s < 300:
		res.Outcome = Accepted
		var accepted v1.ReportAccepted
		if json.Unmarshal(body, &accepted) == nil {
			res.ID = accepted.ID
		}
		return res
	case s == http.StatusUnauthorized || s == http.StatusForbidden:
		res.Outcome = Unauthorized
	case s == http.StatusBadRequest || s == http.StatusConflict || s == http.StatusGone:
		res.Outcome = Rejected
	default:
		res.Outcome = Retry
	}
	res.Message = problemMessage(body)
	return res
}

func problemMessage(body []byte) string {
	var p v1.Problem
	if json.Unmarshal(body, &p) == nil && (p.Title != "" || p.Detail != "") {
		if p.Detail == "" {
			return p.Title
		}
		if p.Title == "" {
			return p.Detail
		}
		return p.Title + ": " + p.Detail
	}
	text := strings.TrimSpace(string(body))
	if len(text) > 300 {
		text = text[:300]
	}
	return text
}

// Advice is the log line's next step for a refusal.
func Advice(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "the token is not valid: create an organization API token, update agent.token (or the secret in agent.secret), and restart the agent; buffered reports are kept and sent then"
	case http.StatusForbidden:
		return "the token lacks the Report Kubernetes usage capability, or is a workspace token: use an organization API token with that capability and restart the agent; buffered reports are kept and sent then"
	case http.StatusConflict:
		return "another agent instance reports this cluster ID for an overlapping window, or the organization reached its cluster limit: run one agent per cluster, with a unique agent.clusterID"
	case http.StatusGone:
		return "this cluster was deleted in Costfluent: install with a new agent.clusterID to report it again"
	case http.StatusBadRequest:
		return "the API refused the report as invalid; upgrade the agent, and contact support if it persists"
	case http.StatusRequestEntityTooLarge:
		return "the report is larger than the API accepts; retrying with backoff"
	default:
		return "retrying with backoff"
	}
}
