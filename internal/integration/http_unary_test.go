package integration_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/connector"
	"github.com/terion-name/airpc/internal/edge"
)

func TestHTTPUnaryEndToEnd(t *testing.T) {
	natsURL := startNATS(t)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("backend method = %s", r.Method)
		}
		if got := r.URL.RequestURI(); got != "/v1/resource?x=1" {
			t.Fatalf("backend request URI = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read backend request body: %v", err)
		}
		if string(body) != "request-body" {
			t.Fatalf("backend body = %q", body)
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Fatalf("Accept header = %q", r.Header.Get("Accept"))
		}
		if r.Header.Get("Authorization") != "" {
			t.Fatalf("Authorization should not be forwarded")
		}
		w.Header().Set("X-Backend", "ok")
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("response-body"))
	}))
	defer backend.Close()

	cfg := config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{{
			Name:                   "demo",
			Mode:                   config.ModeHTTP,
			PublicPrefix:           "/demo",
			Target:                 backend.URL,
			MaxInlineRequestBytes:  config.Bytes(1024),
			MaxInlineResponseBytes: config.Bytes(1024),
			Timeout:                config.Duration{Duration: 5 * time.Second},
			ForwardedHeaders:       []string{"accept", "content-type", "x-request-id"},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := connector.Start(ctx, cfg, "test-1"); err != nil {
		t.Fatalf("connector.Start(): %v", err)
	}
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, "http://"+edgeServer.Addr()+"/demo/v1/resource?x=1", strings.NewReader("request-body"))
	if err != nil {
		t.Fatalf("NewRequest(): %v", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "secret")
	req.Header.Set("Connection", "keep-alive")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("edge request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read edge response: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d body = %q", resp.StatusCode, body)
	}
	if string(body) != "response-body" {
		t.Fatalf("body = %q", body)
	}
	if resp.Header.Get("X-Backend") != "ok" {
		t.Fatalf("X-Backend = %q", resp.Header.Get("X-Backend"))
	}
	if resp.Header.Get("Connection") != "" {
		t.Fatalf("Connection should be stripped")
	}
}

func startNATS(t *testing.T) string {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("new NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatalf("NATS server did not become ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}
