// Command proxy is the forward proxy the report-proxy e2e test sends the agent's report traffic
// through. It tunnels a CONNECT to PROXY_TARGET, the receiver's host and port, that carries
// PROXY_USER and PROXY_PASSWORD as Basic Proxy-Authorization, and refuses every other request with
// 407, 403 or 405 and counts it, so an agent that ignores the proxy's credentials, or sends it
// traffic other than reports, fails the run. GET /stats, the one non-CONNECT route, serves the
// tunnels opened and the refusals as JSON. It listens on :3128.
package main

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

const dialTimeout = 10 * time.Second

type proxy struct {
	credentials string
	// target is the one host:port tunnelled to. The proxy dials it, never the request's host, so it
	// reaches nothing else whatever it is asked.
	target string

	mu       sync.Mutex
	tunnels  int
	refusals []string
}

// refuse answers status and records reason, which stats serves. The log line leaves the request's
// values out, so a request cannot write into it.
func (p *proxy) refuse(w http.ResponseWriter, status int, reason string) {
	p.mu.Lock()
	p.refusals = append(p.refusals, reason)
	p.mu.Unlock()
	log.Printf("refused a request: %d %s", status, http.StatusText(status))
	if status == http.StatusProxyAuthRequired {
		w.Header().Set("Proxy-Authenticate", `Basic realm="e2e"`)
	}
	http.Error(w, reason, status)
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/stats" {
		p.mu.Lock()
		defer p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Tunnels  int      `json:"tunnels"`
			Refusals []string `json:"refusals"`
		}{p.tunnels, p.refusals})
		return
	}
	if r.Method != http.MethodConnect {
		p.refuse(w, http.StatusMethodNotAllowed, r.Method+" "+r.URL.String()+": only CONNECT is proxied")
		return
	}
	got := []byte(r.Header.Get("Proxy-Authorization"))
	if subtle.ConstantTimeCompare(got, []byte(p.credentials)) != 1 {
		p.refuse(w, http.StatusProxyAuthRequired, "CONNECT "+r.Host+": missing or wrong proxy credentials")
		return
	}
	if r.Host != p.target {
		p.refuse(w, http.StatusForbidden, "CONNECT "+r.Host+": only "+p.target+" is tunnelled to")
		return
	}
	upstream, err := net.DialTimeout("tcp", p.target, dialTimeout)
	if err != nil {
		p.refuse(w, http.StatusBadGateway, "CONNECT "+p.target+": "+err.Error())
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		p.refuse(w, http.StatusInternalServerError, "CONNECT "+p.target+": the connection cannot be hijacked")
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		log.Printf("CONNECT %s: hijack: %v", p.target, err)
		return
	}
	// The server's header deadline may still be set on the connection, and a tunnel outlives it.
	_ = client.SetDeadline(time.Time{})
	p.mu.Lock()
	p.tunnels++
	p.mu.Unlock()
	log.Printf("tunnel to %s", p.target)
	if _, err := io.WriteString(client, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	go func() {
		// Bytes the client sent after the CONNECT, the TLS ClientHello among them, may already sit in
		// the server's read buffer, so the upstream copy starts from it rather than from the raw conn.
		_, _ = io.Copy(upstream, buffered)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(client, upstream)
	_ = client.Close()
}

func newProxy(user, password, target string) *proxy {
	return &proxy{
		credentials: "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password)),
		target:      target,
	}
}

func main() {
	user, password, target := os.Getenv("PROXY_USER"), os.Getenv("PROXY_PASSWORD"), os.Getenv("PROXY_TARGET")
	if user == "" || password == "" || target == "" {
		log.Fatal("PROXY_USER, PROXY_PASSWORD and PROXY_TARGET are required")
	}
	srv := &http.Server{Addr: ":3128", Handler: newProxy(user, password, target), ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
