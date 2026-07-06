package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/terion-name/airpc/internal/config"
)

func httpRouteConfig(natsURL, backendURL string, inlineLimit config.Bytes) config.Config {
	return config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{{
			Name:                   "stream",
			Mode:                   config.ModeHTTP,
			PublicPrefix:           "/stream",
			Target:                 backendURL,
			MaxInlineResponseBytes: inlineLimit,
			Timeout:                config.Duration{Duration: 5 * time.Second},
		}},
	}
}

// TestHTTPResponseStreamingSSE proves that server-sent events flow through
// incrementally: the client must receive the first event while the backend is
// still holding the response open.
func TestHTTPResponseStreamingSSE(t *testing.T) {
	natsURL := startNATS(t)

	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: first\n\n")
		flusher.Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: second\n\n")
	}))
	defer backend.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, httpRouteConfig(natsURL, backend.URL, 0), "test-sse")

	resp, err := http.Get("http://" + edgeServer.Addr() + "/stream/events")
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q", got)
	}

	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadString('\n')
	if err != nil || first != "data: first\n" {
		t.Fatalf("first event = %q, err = %v (backend still blocked: streaming is not incremental)", first, err)
	}
	// Only after the first event arrived may the backend finish.
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read remaining events: %v", err)
	}
	if !bytes.Contains(rest, []byte("data: second")) {
		t.Fatalf("remaining events = %q", rest)
	}
}

// TestHTTPResponseStreamingLargeBody proves that responses above
// max_inline_response stream through the tunnel instead of failing.
func TestHTTPResponseStreamingLargeBody(t *testing.T) {
	natsURL := startNATS(t)

	payload := make([]byte, 8<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer backend.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 64 KiB inline limit forces the tunnel path for the 8 MiB body.
	edgeServer := startStack(t, ctx, httpRouteConfig(natsURL, backend.URL, config.Bytes(64<<10)), "test-large")

	resp, err := http.Get("http://" + edgeServer.Addr() + "/stream/blob")
	if err != nil {
		t.Fatalf("GET blob: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("body = %d bytes, want %d matching bytes", len(body), len(payload))
	}
}

// TestHTTPResponseStreamingFallsBackInline proves that a connector without a
// live data tunnel still serves responses that fit inline.
func TestHTTPResponseStreamingFallsBackInline(t *testing.T) {
	natsURL := startNATS(t)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Chunked (no Content-Length) but small: eligible for streaming,
		// must fall back to inline when the tunnel is down.
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "inline-fallback")
	}))
	defer backend.Close()

	cfg := httpRouteConfig(natsURL, backend.URL, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Keep the bogus data URL so the connector tunnel never connects.
	edgeServer := startStackWithoutTunnel(t, ctx, cfg, "test-fallback")

	resp, err := http.Get("http://" + edgeServer.Addr() + "/stream/small")
	if err != nil {
		t.Fatalf("GET small: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "inline-fallback" {
		t.Fatalf("body = %q, err = %v", body, err)
	}
}
