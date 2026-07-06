package integration_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/connector"
	"github.com/terion-name/airpc/internal/edge"
)

// streamConfig builds a single-TCP-route config; DataAddr and EdgeDataURL are
// wired by startStack unless a test overrides them.
func streamConfig(natsURL, routeName, backendAddr string) config.Config {
	return config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{{
			Name:    routeName,
			Mode:    config.ModeTCP,
			Listen:  "127.0.0.1:0",
			Target:  backendAddr,
			Timeout: config.Duration{Duration: 5 * time.Second},
		}},
	}
}

func startStack(t *testing.T, ctx context.Context, cfg config.Config, connectorID string) *edge.Server {
	t.Helper()
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	cfg.Connector.EdgeDataURL = "ws://" + edgeServer.DataAddr() + "/_airpc/data"
	startConnectorOrFatal(t, ctx, cfg, connectorID)
	return edgeServer
}

func startConnectorOrFatal(t *testing.T, ctx context.Context, cfg config.Config, id string) {
	t.Helper()
	if _, err := connector.Start(ctx, cfg, id); err != nil {
		t.Fatalf("connector.Start(): %v", err)
	}
}

// startStackWithoutTunnel leaves the connector's data URL unreachable so its
// tunnel never connects.
func startStackWithoutTunnel(t *testing.T, ctx context.Context, cfg config.Config, connectorID string) *edge.Server {
	t.Helper()
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	startConnectorOrFatal(t, ctx, cfg, connectorID)
	return edgeServer
}

// TestTCPRelayHalfClose proves that a client's write-side FIN reaches the
// backend while the response direction stays open: the backend reads the full
// request until EOF, then sends the response back.
func TestTCPRelayHalfClose(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPReadAllThenEcho(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, streamConfig(natsURL, "halfclose_tcp", backend), "test-halfclose")

	conn, err := net.DialTimeout("tcp", edgeServer.TCPAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial edge tcp: %v", err)
	}
	defer conn.Close()

	request := make([]byte, 1<<20)
	for i := range request {
		request[i] = byte(i % 251)
	}
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("half-close: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !bytes.Equal(response, request) {
		t.Fatalf("response = %d bytes, want %d matching bytes", len(response), len(request))
	}
}

// TestTCPRelaySessionIsolation proves per-session flow control: a session
// whose client stops reading must not stall other sessions on the same
// connector data tunnel.
func TestTCPRelaySessionIsolation(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPRawEcho(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, streamConfig(natsURL, "iso_tcp", backend), "test-iso")
	edgeAddr := edgeServer.TCPAddrs()[0]

	// Session A: flood without ever reading the echo, wedging the session
	// once its send window and socket buffers fill.
	stalled, err := net.DialTimeout("tcp", edgeAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial stalled session: %v", err)
	}
	t.Cleanup(func() { _ = stalled.Close() })
	go func() {
		flood := make([]byte, 64<<20)
		_, _ = stalled.Write(flood) // blocks; released by Cleanup close
	}()
	time.Sleep(500 * time.Millisecond)

	// Session B on the same tunnel must still complete promptly.
	probe, err := net.DialTimeout("tcp", edgeAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial probe session: %v", err)
	}
	defer probe.Close()
	if _, err := probe.Write([]byte("hol-check")); err != nil {
		t.Fatalf("write probe: %v", err)
	}
	if err := probe.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set probe deadline: %v", err)
	}
	got := make([]byte, len("hol-check"))
	if _, err := io.ReadFull(probe, got); err != nil {
		t.Fatalf("probe echo blocked by stalled session: %v", err)
	}
	if string(got) != "hol-check" {
		t.Fatalf("probe echo = %q", got)
	}
}

// TestTCPRelayIdleTimeout proves that an idle session is torn down once
// idle_timeout elapses.
func TestTCPRelayIdleTimeout(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPRawEcho(t)

	cfg := streamConfig(natsURL, "idle_tcp", backend)
	cfg.Routes[0].IdleTimeout = config.Duration{Duration: 300 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, cfg, "test-idle")

	conn, err := net.DialTimeout("tcp", edgeServer.TCPAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial edge tcp: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, 4)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}

	// Stay quiet past the idle timeout; the edge must close the connection.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := conn.Read(got); err == nil {
		t.Fatalf("connection still open after idle timeout")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("idle session was not closed within 5s")
	}
}

// TestTunnelReconnect proves that the connector redials the edge data tunnel
// after the edge restarts, restoring stream routes without a connector
// restart.
func TestTunnelReconnect(t *testing.T) {
	natsURL := startNATS(t)
	backend := startTCPRawEcho(t)

	// The data listener must come back on the same address, so reserve a
	// fixed port instead of :0.
	dataAddr := reserveAddr(t)
	cfg := streamConfig(natsURL, "reconnect_tcp", backend)
	cfg.Edge.DataAddr = dataAddr
	cfg.Connector.EdgeDataURL = "ws://" + dataAddr + "/_airpc/data"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edge1, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	if _, err := connector.Start(ctx, cfg, "test-reconnect"); err != nil {
		t.Fatalf("connector.Start(): %v", err)
	}
	if err := tryEcho(edge1.TCPAddrs()[0]); err != nil {
		t.Fatalf("echo before restart: %v", err)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = edge1.Close(closeCtx)
	closeCancel()
	if err := edge1.Wait(); err != nil {
		t.Fatalf("edge shutdown: %v", err)
	}

	edge2, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("restart edge: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := tryEcho(edge2.TCPAddrs()[0])
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("echo after restart: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tryEcho(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("echo")); err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(conn, got); err != nil {
		return err
	}
	return nil
}

func reserveAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve addr: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

// startTCPReadAllThenEcho serves connections that read the entire request
// until EOF and only then write it back, exercising half-close propagation.
func startTCPReadAllThenEcho(t *testing.T) string {
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
				data, err := io.ReadAll(conn)
				if err != nil {
					return
				}
				_, _ = conn.Write(data)
			}(conn)
		}
	}()
	return listener.Addr().String()
}
