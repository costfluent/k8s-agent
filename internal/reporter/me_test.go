package reporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
)

func meServer(t *testing.T, status int, body string, seen *http.Request) *url.URL {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen != nil {
			*seen = *r.Clone(context.Background())
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL + "/api/")
	return u
}

func TestClient_MeDecodesTheToken(t *testing.T) {
	var seen http.Request
	endpoint := meServer(t, http.StatusOK, `{"organizationId":"org_1","tokenType":"cluster","tokenName":"prod-eu",
		"capabilities":["reportKubernetesUsage"],"clusterId":"prod-eu"}`, &seen)

	me, res := NewClient(endpoint, "cf_org_secret", "1.2.3", nil).Me(context.Background())
	if res.Outcome != Accepted || res.Status != http.StatusOK {
		t.Fatalf("result = %+v, want accepted", res)
	}
	if me.TokenType != "cluster" || me.TokenName != "prod-eu" || me.ClusterID != "prod-eu" ||
		!slices.Equal(me.Capabilities, []string{"reportKubernetesUsage"}) {
		t.Fatalf("me = %+v", me)
	}
	if seen.Method != http.MethodGet || seen.URL.Path != "/api/v1/me" {
		t.Errorf("request = %s %s, want GET /api/v1/me under the endpoint's path", seen.Method, seen.URL.Path)
	}
	if seen.Header.Get("Authorization") != "Bearer cf_org_secret" || seen.Header.Get("User-Agent") != "costfluent-k8s-agent/1.2.3" {
		t.Errorf("headers = %v", seen.Header)
	}
}

func TestClient_MeClassifiesARefusal(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		outcome Outcome
		message string
	}{
		{"invalid token", http.StatusUnauthorized, `{"title":"Authentication Error","detail":"The API token is invalid"}`, Unauthorized, "The API token is invalid"},
		{"refused token", http.StatusForbidden, `{"title":"Forbidden","detail":"API tokens are not included in the Free plan"}`, Unauthorized, "not included"},
		{"server error", http.StatusBadGateway, `{"title":"Bad Gateway"}`, Retry, "Bad Gateway"},
		{"rate limited", http.StatusTooManyRequests, `{"title":"Too Many Requests"}`, Retry, "Too Many Requests"},
		{"unreadable token", http.StatusOK, `<html>`, Retry, "GET /v1/me answered"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			me, res := NewClient(meServer(t, tc.status, tc.body, nil), "t", "v", nil).Me(context.Background())
			if res.Outcome != tc.outcome || res.Status != tc.status || !strings.Contains(res.Message, tc.message) {
				t.Fatalf("result = %+v, want outcome %v with %q", res, tc.outcome, tc.message)
			}
			if me.TokenType != "" || me.Capabilities != nil {
				t.Errorf("me = %+v on a refusal", me)
			}
		})
	}
}

func TestClient_MeRetriesAnAPIItCannotReach(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	u, _ := url.Parse(srv.URL)
	if _, res := NewClient(u, "t", "v", nil).Me(context.Background()); res.Outcome != Retry || !strings.HasPrefix(res.Message, "GET ") {
		t.Fatalf("result = %+v, want a retry naming the request", res)
	}

	bad := &url.URL{Scheme: "http", Host: "bad host"}
	if _, res := NewClient(bad, "t", "v", nil).Me(context.Background()); res.Outcome != Retry || !strings.HasPrefix(res.Message, "building request:") {
		t.Fatalf("result = %+v", res)
	}
}
