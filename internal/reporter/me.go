package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const mePath = "/v1/me"

// Me is the API token the agent reports with, as GET /v1/me describes it. The agent reads it, and
// sends nothing of it back. A cluster token names the cluster it reports for in ClusterID, the
// cluster's agent.clusterID; any other token leaves it empty.
type Me struct {
	TokenType    string   `json:"tokenType"`
	TokenName    string   `json:"tokenName"`
	Capabilities []string `json:"capabilities"`
	ClusterID    string   `json:"clusterId"`
}

// Me reads the token the client reports with. Its Result is Accepted when Me holds the token, and
// classifies anything else as a send's: Unauthorized for 401 and 403, Retry for a network error, a
// 429, a 5xx or an answer that is not a token.
func (c *Client) Me(ctx context.Context) (Me, Result) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+mePath, nil)
	if err != nil {
		return Me{}, Result{Outcome: Retry, Message: fmt.Sprintf("building request: %v", err)}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return Me{}, Result{Outcome: Retry, Message: fmt.Sprintf("GET %s: %v", c.base+mePath, err)}
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))

	res := Result{Status: resp.StatusCode}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.classify(&res, resp, body)
		return Me{}, res
	}
	var me Me
	if err := json.Unmarshal(body, &me); err != nil {
		res.Outcome, res.Message = Retry, fmt.Sprintf("GET /v1/me answered %d with a body that is not a token: %v", resp.StatusCode, err)
		return Me{}, res
	}
	res.Outcome = Accepted
	return me, res
}
