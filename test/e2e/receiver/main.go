// Command receiver is the stub Costfluent API the kind smoke test points the agent at. It accepts
// POST /v1/kubernetes/reports only when the body is gzip and the bearer token matches
// RECEIVER_TOKEN, keeps each accepted body, and serves them back as a JSON array on GET /reports.
// Anything else it refuses and counts, so the test can tell a wrong request from no request.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const maxBody = 64 << 20

type store struct {
	mu       sync.Mutex
	reports  []json.RawMessage
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
	s := &store{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/kubernetes/reports", func(w http.ResponseWriter, r *http.Request) {
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
		s.mu.Lock()
		s.reports = append(s.reports, json.RawMessage(bytes.Clone(body)))
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
			Refusals []string          `json:"refusals"`
		}{s.reports, s.refusals})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
