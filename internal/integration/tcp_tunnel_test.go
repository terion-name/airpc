package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/connector"
	"github.com/terion-name/airpc/internal/edge"
)

func TestTCPRelayEndToEnd(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPEcho(t)

	cfg := config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data", TunnelToken: "test-token"},
		Routes: []config.Route{{
			Name:    "echo_tcp",
			Mode:    config.ModeTCP,
			Listen:  "127.0.0.1:0",
			Target:  backend,
			Timeout: config.Duration{Duration: 5 * time.Second},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	cfg.Connector.EdgeDataURL = "ws://" + edgeServer.DataAddr() + "/_airpc/data"
	if _, err := connector.Start(ctx, cfg, "test-1"); err != nil {
		t.Fatalf("connector.Start(): %v", err)
	}

	addrs := edgeServer.TCPAddrs()
	if len(addrs) != 1 {
		t.Fatalf("TCPAddrs() = %v", addrs)
	}
	conn, err := net.DialTimeout("tcp", addrs[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial edge tcp: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello tunnel\n")); err != nil {
		t.Fatalf("write edge tcp: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read edge tcp: %v", err)
	}
	if line != "HELLO TUNNEL\n" {
		t.Fatalf("line = %q", line)
	}
}

func TestTCPRelayBulkEcho(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPRawEcho(t)

	cfg := config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{{
			Name:    "bulk_tcp",
			Mode:    config.ModeTCP,
			Listen:  "127.0.0.1:0",
			Target:  backend,
			Timeout: config.Duration{Duration: 5 * time.Second},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	cfg.Connector.EdgeDataURL = "ws://" + edgeServer.DataAddr() + "/_airpc/data"
	if _, err := connector.Start(ctx, cfg, "test-bulk"); err != nil {
		t.Fatalf("connector.Start(): %v", err)
	}

	conn, err := net.DialTimeout("tcp", edgeServer.TCPAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial edge tcp: %v", err)
	}
	defer conn.Close()

	// The payload must exceed the total kernel socket and relay channel
	// buffering along the echo path, so that the delayed read below forces
	// the tunnel to hold back frames instead of absorbing them.
	payload := make([]byte, 32<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(payload)
		writeErr <- err
	}()

	// Delay reading so the echoed bytes back up behind the tunnel; dropped
	// frames surface as a short read below.
	time.Sleep(500 * time.Millisecond)

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echoed payload differs from sent payload")
	}
}

func startTCPRawEcho(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp backend: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(conn)
		}
	}()
	return listener.Addr().String()
}

func startTCPEcho(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp backend: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					_, _ = conn.Write([]byte(strings.ToUpper(scanner.Text()) + "\n"))
				}
			}(conn)
		}
	}()
	return listener.Addr().String()
}
