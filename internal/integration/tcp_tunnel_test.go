package integration_test

import (
	"bufio"
	"context"
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
