package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// echo serves one listener that writes back every line it reads, the upstream a tunnel reaches.
func echo(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

// connect sends a CONNECT for target with the given Proxy-Authorization, "" sending none, and
// returns the connection and the proxy's response.
func connect(t *testing.T, proxyAddr, target, auth string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	req := "CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n"
	if auth != "" {
		req += "Proxy-Authorization: " + auth + "\r\n"
	}
	if _, err := io.WriteString(conn, req+"\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	return conn, reader, resp
}

func basic(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}

func stats(t *testing.T, base string) (tunnels int, refusals []string) {
	t.Helper()
	resp, err := http.Get(base + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var got struct {
		Tunnels  int      `json:"tunnels"`
		Refusals []string `json:"refusals"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	return got.Tunnels, got.Refusals
}

func TestProxyTunnelsOnlyToItsTargetWithItsCredentials(t *testing.T) {
	upstream := echo(t)
	srv := httptest.NewServer(newProxy("agent", "s3cret", upstream))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	for name, auth := range map[string]string{
		"no credentials": "",
		"wrong password": basic("agent", "wrong"),
		"wrong user":     basic("other", "s3cret"),
		"not basic":      "Bearer s3cret",
	} {
		_, _, resp := connect(t, addr, upstream, auth)
		if resp.StatusCode != http.StatusProxyAuthRequired {
			t.Errorf("%s: CONNECT answered %s, want 407", name, resp.Status)
		}
		if got := resp.Header.Get("Proxy-Authenticate"); got != `Basic realm="e2e"` {
			t.Errorf("%s: Proxy-Authenticate = %q", name, got)
		}
	}

	resp, err := http.Get(srv.URL + "/v1/me")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET through the proxy answered %s, want 405", resp.Status)
	}
	other := echo(t)
	_, _, resp = connect(t, addr, other, basic("agent", "s3cret"))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("CONNECT with the credentials to %s, not the target, answered %s, want 403", other, resp.Status)
	}
	if tunnels, refusals := stats(t, srv.URL); tunnels != 0 || len(refusals) != 6 {
		t.Fatalf("after six refused requests: %d tunnels, refusals %v", tunnels, refusals)
	}

	conn, reader, resp := connect(t, addr, upstream, basic("agent", "s3cret"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT with the credentials answered %s, want 200", resp.Status)
	}
	if _, err := io.WriteString(conn, "through the tunnel\n"); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "through the tunnel\n" {
		t.Fatalf("read back %q, %v", line, err)
	}
	if tunnels, _ := stats(t, srv.URL); tunnels != 1 {
		t.Fatalf("%d tunnels, want 1", tunnels)
	}
}

func TestProxyAnswers502WhenItsTargetIsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	_ = ln.Close()
	srv := httptest.NewServer(newProxy("agent", "s3cret", closed))
	t.Cleanup(srv.Close)

	_, _, resp := connect(t, strings.TrimPrefix(srv.URL, "http://"), closed, basic("agent", "s3cret"))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("CONNECT to a closed port answered %s, want 502", resp.Status)
	}
}
