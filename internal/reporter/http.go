// Package reporter posts buffered reports to POST {endpoint}/v1/kubernetes/reports, and reads the
// token it posts them with from GET {endpoint}/v1/me.
package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/storage"
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
	// Rejected: the API refused this report for good (400, 409, 410, 413); remove it. The sender
	// retries a window_end refused as in the future instead (Result.refusedAsFuture).
	Rejected
	// Unauthorized: the token is wrong or lacks the capability (401, 403). Keep the report and stop
	// sending until the agent restarts with a fixed token.
	Unauthorized
	// Retry: a transient failure (network, 429, 5xx); keep the report and back off, at least as long
	// as the API's Retry-After asks.
	Retry
)

// Result is one attempt's outcome and what the API said.
type Result struct {
	Outcome Outcome
	Status  int
	Message string
	ID      string
	// Refused names the fields the API refused, when it said which.
	Refused []string
	// RetryAfter is how long the API asked the agent to wait, from its Retry-After header.
	RetryAfter time.Duration
	// ServerTime is the response's Date, zero when it has none or it cannot be read.
	ServerTime time.Time
	// ClockAhead is how far this node's clock ran ahead of ServerTime when the request was sent, or 0.
	ClockAhead time.Duration
}

// Client sends reports.
type Client struct {
	// base is the endpoint every path is under; url is the reports path under it.
	base      string
	url       string
	token     string
	userAgent string
	http      *http.Client
	now       func() time.Time
}

// NewClient builds a client for endpoint. proxy, when set, carries only report traffic; otherwise
// the standard proxy environment variables apply.
func NewClient(endpoint *url.URL, token, version string, proxy *url.URL) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxy != nil {
		transport.Proxy = http.ProxyURL(proxy)
	}
	base := strings.TrimRight(endpoint.String(), "/")
	return &Client{
		base:      base,
		url:       base + reportsPath,
		token:     token,
		userAgent: "costfluent-k8s-agent/" + version,
		http:      &http.Client{Transport: transport, Timeout: requestTimeout},
		now:       time.Now,
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

	sent := c.now()
	resp, err := c.http.Do(req)
	if err != nil {
		return Result{Outcome: Retry, Message: fmt.Sprintf("POST %s: %v", c.url, err)}
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))

	res := Result{Status: resp.StatusCode}
	if at, err := http.ParseTime(resp.Header.Get("Date")); err == nil {
		res.ServerTime = at
		res.ClockAhead = max(sent.Sub(at).Round(time.Second), 0)
	}
	if s := resp.StatusCode; s >= 200 && s < 300 {
		res.Outcome = Accepted
		var accepted v1.ReportAccepted
		if json.Unmarshal(body, &accepted) == nil {
			res.ID = accepted.ID
		}
		return res
	}
	c.classify(&res, resp, body)
	return res
}

// classify sets the outcome of an answer that is not 2xx, and what the API said. Only the refusals
// that repeat for the same report drop it; anything else is retried, including the 422 the API
// answers for a schema_version it does not read yet, so a report sent before the API that reads it is
// deployed is kept until then.
func (c *Client) classify(res *Result, resp *http.Response, body []byte) {
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		res.Outcome = Unauthorized
	case http.StatusBadRequest, http.StatusConflict, http.StatusGone, http.StatusRequestEntityTooLarge:
		res.Outcome = Rejected
	default:
		res.Outcome = Retry
		res.RetryAfter = retryAfter(resp.Header.Get("Retry-After"), c.now())
	}
	var p v1.Problem
	if json.Unmarshal(body, &p) == nil {
		for _, e := range p.Errors {
			res.Refused = append(res.Refused, e.Name)
		}
	}
	res.Message = problemMessage(p, body)
}

// refusedAsFuture reports whether a 400 refusing window_end is the API's "in the future" refusal of
// a window ending at windowEnd, which it takes once its clock passes that end. That refusal implies
// windowEnd lies beyond the API's clock, so its Date; any other window_end refusal stops being
// retried once the Date passes windowEnd, minutes at most, and the buffer's oldest-first drain is
// not held behind it.
func (r Result) refusedAsFuture(windowEnd time.Time) bool {
	return r.Status == http.StatusBadRequest && !r.ServerTime.IsZero() &&
		refused(r.Refused, "windowEnd") && windowEnd.After(r.ServerTime)
}

// retryAfter reads a Retry-After value, delay-seconds or an HTTP date, as a wait from now. A value
// it cannot read, or one already past, is no wait.
func retryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(max(seconds, 0)) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(at.Sub(now), 0)
	}
	return 0
}

func problemMessage(p v1.Problem, body []byte) string {
	parts := make([]string, 0, 2)
	for _, s := range []string{p.Title, p.Detail} {
		if s != "" {
			parts = append(parts, s)
		}
	}
	reasons := make([]string, 0, len(p.Errors))
	for _, e := range p.Errors {
		if e.Reason != "" {
			reasons = append(reasons, e.Reason)
		}
	}
	if len(reasons) > 0 {
		parts = append(parts, strings.Join(reasons, "; "))
	}
	text := strings.Join(parts, ": ")
	if text == "" {
		text = strings.TrimSpace(string(body))
	}
	if len(text) > 300 {
		text = text[:300]
	}
	return text
}

// Advice is the log line's next step for a refusal.
func Advice(res Result) string {
	switch res.Status {
	case http.StatusUnauthorized:
		return "the token is not valid: it may have been revoked, or created by an account that is now disabled or deleted; use Mint token on the cluster in Costfluent, put the new token in agent.token (or the secret in agent.secret), restart the agent, then revoke the old one; buffered reports are kept and sent then"
	case http.StatusForbidden:
		return "the token is not this cluster's token: use Mint token on the cluster in Costfluent, put that token in agent.token (or the secret in agent.secret), and restart the agent; buffered reports are kept and sent then"
	case http.StatusConflict:
		return "another agent instance reports this cluster for an overlapping window: run one agent per cluster, each with its own cluster's token"
	case http.StatusGone:
		return "this cluster was deleted in Costfluent: add it again under Kubernetes, Clusters, and install its new token, or uninstall the agent"
	case http.StatusBadRequest:
		if res.Outcome == Retry {
			kept := fmt.Sprintf("the report is kept and retried until the API's clock passes its window_end, for up to %.0f hours", storage.DefaultMaxAge.Hours())
			if res.ClockAhead > 0 {
				return "this node's clock is " + res.ClockAhead.String() + " ahead of Costfluent's, so the API refused the report's window_end as in the future: fix the node's time sync (NTP); " + kept
			}
			return "the API refused the report's window_end as in the future, as it was built while this node's clock ran ahead; " + kept
		}
		if refusedSize(res.Refused) {
			return "the cluster has more nodes or pods than Costfluent collects from one cluster (the message names the limit); upgrading the agent does not change it, contact support"
		}
		return "the API refused the report as invalid; upgrade the agent, and contact support if it persists"
	case http.StatusRequestEntityTooLarge:
		return "the report is larger than the API accepts; it is dropped, because a retry is refused the same way; contact support if it persists"
	default:
		return "retrying with backoff"
	}
}

// refusedSize reports whether the API refused the report's node or pod count (report.nodes), as
// opposed to a field of one node or pod (report.pods[3].containers).
func refusedSize(fields []string) bool {
	return refused(fields, "nodes") || refused(fields, "pods")
}

// refused reports whether any of fields ends in name, in any case: report.windowEnd or
// Report.WindowEnd for windowEnd.
func refused(fields []string, name string) bool {
	for _, f := range fields {
		if strings.EqualFold(f[strings.LastIndex(f, ".")+1:], name) {
			return true
		}
	}
	return false
}
