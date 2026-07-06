package integration_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/terion-name/airpc/internal/config"
)

// TestMetricsAndGRPCObservability drives real HTTP and gRPC traffic through
// the edge and asserts the Prometheus endpoint reports it — including the
// passively observed gRPC method and status from the opaque relay.
func TestMetricsAndGRPCObservability(t *testing.T) {
	natsURL := startNATS(t)

	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc backend: %v", err)
	}
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	go func() { _ = grpcServer.Serve(grpcListener) }()
	t.Cleanup(grpcServer.Stop)

	httpBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer httpBackend.Close()

	cfg := config.Config{
		NATS:      config.NATSConfig{URL: natsURL},
		Edge:      config.EdgeConfig{HTTPAddr: "127.0.0.1:0", DataAddr: "127.0.0.1:0", MetricsAddr: "127.0.0.1:0"},
		Connector: config.ConnectorConfig{EdgeDataURL: "ws://127.0.0.1:1/_airpc/data"},
		Routes: []config.Route{
			{
				Name:         "obs_http",
				Mode:         config.ModeHTTP,
				PublicPrefix: "/obs",
				Target:       httpBackend.URL,
				Timeout:      config.Duration{Duration: 5 * time.Second},
			},
			{
				Name:    "obs_grpc",
				Mode:    config.ModeGRPC,
				Listen:  "127.0.0.1:0",
				Target:  grpcListener.Addr().String(),
				Timeout: config.Duration{Duration: 5 * time.Second},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer := startStack(t, ctx, cfg, "test-metrics")

	if resp, err := http.Get("http://" + edgeServer.Addr() + "/obs/"); err != nil {
		t.Fatalf("http request: %v", err)
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	grpcConn, err := grpc.NewClient(edgeServer.TCPAddrs()[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	defer grpcConn.Close()
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer checkCancel()
	if _, err := healthpb.NewHealthClient(grpcConn).Check(checkCtx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check through relay: %v", err)
	}

	want := []string{
		`airpc_http_requests_total{route="obs_http",status="200"} 1`,
		`airpc_stream_sessions_total{mode="grpc",route="obs_grpc"} 1`,
		`airpc_grpc_rpcs_total{code="0",method="/grpc.health.v1.Health/Check",route="obs_grpc"} 1`,
	}
	// The gRPC observer records asynchronously; poll the scrape briefly.
	deadline := time.Now().Add(5 * time.Second)
	for {
		body := scrapeMetrics(t, edgeServer.MetricsAddr())
		missing := ""
		for _, line := range want {
			if !strings.Contains(body, line) {
				missing = line
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric %q not found in scrape:\n%s", missing, relevantMetricLines(body))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func scrapeMetrics(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(body)
}

func relevantMetricLines(body string) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "airpc_") {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
