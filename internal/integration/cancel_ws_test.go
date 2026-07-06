package integration_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/connector"
	"github.com/terion-name/airpc/internal/edge"
)

// TestHTTPUnaryCancellation proves that a client disconnect reaches the
// backend as context cancellation instead of occupying it until the route
// timeout.
func TestHTTPUnaryCancellation(t *testing.T) {
	natsURL := startNATS(t)

	backendCanceled := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(backendCanceled)
	}))
	defer backend.Close()

	cfg := config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{{
			Name:         "slow",
			Mode:         config.ModeHTTP,
			PublicPrefix: "/slow",
			Target:       backend.URL,
			Timeout:      config.Duration{Duration: 30 * time.Second},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, cfg, "test-cancel")

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer reqCancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+edgeServer.Addr()+"/slow/", nil)
	if err != nil {
		t.Fatalf("NewRequest(): %v", err)
	}
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatalf("request unexpectedly succeeded")
	}

	select {
	case <-backendCanceled:
	case <-time.After(5 * time.Second):
		t.Fatalf("backend request was not canceled after client disconnect")
	}
}

// TestOpenRetrySkipsTunnelDownConnector proves that stream opens landing on a
// connector with a dead data tunnel are retried until a healthy connector in
// the same queue group takes the session.
func TestOpenRetrySkipsTunnelDownConnector(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPRawEcho(t)

	cfg := streamConfig(natsURL, "retry_tcp", backend)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}

	// Connector A: data tunnel permanently down, still answering opens.
	downCfg := cfg
	downCfg.Connector.EdgeDataURL = "ws://127.0.0.1:1/_airpc/data"
	if _, err := connector.Start(ctx, downCfg, "test-down"); err != nil {
		t.Fatalf("connector.Start(down): %v", err)
	}
	// Connector B: healthy.
	upCfg := cfg
	upCfg.Connector.EdgeDataURL = "ws://" + edgeServer.DataAddr() + "/_airpc/data"
	if _, err := connector.Start(ctx, upCfg, "test-up"); err != nil {
		t.Fatalf("connector.Start(up): %v", err)
	}

	// A single connection must succeed even when the queue draw first picks
	// the tunnel-down connector.
	conn, err := net.DialTimeout("tcp", edgeServer.TCPAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial edge tcp: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("retry")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("echo through retried open: %v", err)
	}
	if string(got) != "retry" {
		t.Fatalf("echo = %q", got)
	}
}

// TestWebSocketRelayCloseCode proves that a backend-initiated close status
// reaches the public client through the tunnel.
func TestWebSocketRelayCloseCode(t *testing.T) {
	natsURL := startNATS(t)

	upgrader := websocket.Upgrader{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			messageType, payload, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if string(payload) == "bye" {
				deadline := time.Now().Add(time.Second)
				_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4001, "policy"), deadline)
				_, _, _ = ws.ReadMessage() // wait for the close reply
				return
			}
			_ = ws.WriteMessage(messageType, payload)
		}
	}))
	defer backend.Close()

	cfg := config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{{
			Name:       "chat",
			Mode:       config.ModeWebSocket,
			PublicPath: "/chat",
			Target:     "ws" + strings.TrimPrefix(backend.URL, "http"),
			Timeout:    config.Duration{Duration: 5 * time.Second},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, cfg, "test-wsclose")

	client, _, err := websocket.DefaultDialer.Dial("ws://"+edgeServer.Addr()+"/chat", nil)
	if err != nil {
		t.Fatalf("dial edge websocket: %v", err)
	}
	defer client.Close()

	if err := client.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	_, echo, err := client.ReadMessage()
	if err != nil || string(echo) != "hello" {
		t.Fatalf("echo = %q, err = %v", echo, err)
	}

	if err := client.WriteMessage(websocket.TextMessage, []byte("bye")); err != nil {
		t.Fatalf("write bye: %v", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = client.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("expected close error, got %v", err)
	}
	if closeErr.Code != 4001 || closeErr.Text != "policy" {
		t.Fatalf("close = %d %q, want 4001 %q", closeErr.Code, closeErr.Text, "policy")
	}
}
