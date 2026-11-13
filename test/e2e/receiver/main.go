// Command receiver is the stub Costfluent API the e2e tests point the agent at. It describes
// RECEIVER_TOKEN on GET /v1/me as the cluster token of kind-e2e the chart asks for, accepts
// POST /v1/kubernetes/reports only when the body is gzip and the bearer token matches
// RECEIVER_TOKEN, keeps each accepted body with the address of the peer that sent it, and serves
// them back as JSON on GET /reports. Anything else it refuses and counts, so the test can tell a
// wrong request from no request. The peer is the host of the connection's remote address, which is
// the report proxy's when the agent reports through one.
//
// POST /delay makes every later report wait reportDelay after its body arrives and keeps it only
// when the agent is still connected then, so an agent killed before its send completes leaves no
// report behind, however quickly it sent.
//
// POST /unavailable makes every later report that would be accepted answer 503 Service Unavailable
// with no Retry-After instead, neither kept nor counted as a refusal, as the API does while it is
// down; POST /available ends that. GET /v1/me answers as before throughout.
//
// It always serves plain http on :8080. When RECEIVER_TLS_CERT and RECEIVER_TLS_KEY name a PEM
// certificate and key, it also serves the same routes over TLS on :8443, and then refuses
// GET /v1/me and POST /v1/kubernetes/reports that arrive over plain http, so an agent that reports
// without TLS fails the run instead of passing it; /healthz and /reports stay on both. Setting only
// one of the two is a fatal error.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxBody     = 64 << 20
	reportDelay = 3 * time.Second
)

type store struct {
	mu       sync.Mutex
	reports  []json.RawMessage
	peers    []string
	refusals []string
}

func (s *store) refuse(w http.ResponseWriter, status int, reason string) {
	s.mu.Lock()
	s.refusals = append(s.refusals, reason)
	s.mu.Unlock()
	log.Printf("refused: %s", reason)
	http.Error(w, reason, status)
}

func main() {
	token := os.Getenv("RECEIVER_TOKEN")
	if token == "" {
		log.Fatal("RECEIVER_TOKEN is required")
	}
	certFile, keyFile := os.Getenv("RECEIVER_TLS_CERT"), os.Getenv("RECEIVER_TLS_KEY")
	switch {
	case certFile != "" && keyFile == "":
		log.Fatal("RECEIVER_TLS_KEY is required when RECEIVER_TLS_CERT is set")
	case keyFile != "" && certFile == "":
		log.Fatal("RECEIVER_TLS_CERT is required when RECEIVER_TLS_KEY is set")
	}
	withTLS := certFile != ""
	s := &store{}
	var delayed, unavailable atomic.Bool
	plainWhileTLS := func(w http.ResponseWriter, r *http.Request, route string) bool {
		if withTLS && r.TLS == nil {
			s.refuse(w, http.StatusBadRequest, "plain http on "+route+" while TLS is configured")
			return true
		}
		return false
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/me", func(w http.ResponseWriter, r *http.Request) {
		if plainWhileTLS(w, r, "/v1/me") {
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			s.refuse(w, http.StatusUnauthorized, "missing or wrong bearer token on /v1/me")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"organizationId":"org_e2e","tokenType":"cluster","tokenName":"kind-e2e","capabilities":["reportKubernetesUsage"],"clusterId":"kind-e2e"}`)
	})
	mux.HandleFunc("POST /v1/kubernetes/reports", func(w http.ResponseWriter, r *http.Request) {
		if plainWhileTLS(w, r, "/v1/kubernetes/reports") {
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			s.refuse(w, http.StatusUnauthorized, "missing or wrong bearer token")
			return
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			s.refuse(w, http.StatusBadRequest, "body is not gzip-encoded")
			return
		}
		zr, err := gzip.NewReader(io.LimitReader(r.Body, maxBody))
		if err != nil {
			s.refuse(w, http.StatusBadRequest, "gzip: "+err.Error())
			return
		}
		body, err := io.ReadAll(io.LimitReader(zr, maxBody))
		if err != nil {
			s.refuse(w, http.StatusBadRequest, "gzip body: "+err.Error())
			return
		}
		if !json.Valid(body) {
			s.refuse(w, http.StatusBadRequest, "body is not JSON")
			return
		}
		if unavailable.Load() {
			log.Printf("answered a report with 503 while unavailable")
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if delayed.Load() {
			// Over HTTP/1.1 the server notices a closed connection only once the body has been read
			// to EOF, so the wait below sees the agent die only after this, whatever the decoder read.
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-time.After(reportDelay):
			case <-r.Context().Done():
				log.Printf("dropped a report whose sender disconnected during the delay")
				return
			}
		}
		peer, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			peer = r.RemoteAddr
		}
		s.mu.Lock()
		s.reports = append(s.reports, json.RawMessage(bytes.Clone(body)))
		s.peers = append(s.peers, peer)
		n := len(s.reports)
		s.mu.Unlock()
		log.Printf("accepted report %d (%d bytes)", n, len(body))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"id":"e2e","status":"accepted"}`)
	})
	mux.HandleFunc("GET /reports", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Reports  []json.RawMessage `json:"reports"`
			Peers    []string          `json:"peers"`
			Refusals []string          `json:"refusals"`
		}{s.reports, s.peers, s.refusals})
	})
	mux.HandleFunc("POST /delay", func(w http.ResponseWriter, _ *http.Request) {
		delayed.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /unavailable", func(w http.ResponseWriter, _ *http.Request) {
		unavailable.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /available", func(w http.ResponseWriter, _ *http.Request) {
		unavailable.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if !withTLS {
		log.Fatal(srv.ListenAndServe())
	}
	tlsSrv := &http.Server{Addr: ":8443", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- srv.ListenAndServe() }()
	go func() { errs <- tlsSrv.ListenAndServeTLS(certFile, keyFile) }()
	log.Fatal(<-errs)
}
