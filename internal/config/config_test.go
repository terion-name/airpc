package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadExampleConfig(t *testing.T) {
	cfg, err := LoadFile(filepath.Join("..", "..", "examples", "airpc.yaml"))
	if err != nil {
		t.Fatalf("LoadFile(example): %v", err)
	}
	if cfg.NATS.URL != "nats://127.0.0.1:4222" {
		t.Fatalf("nats url = %q", cfg.NATS.URL)
	}
	if cfg.Edge.HTTPAddr != "127.0.0.1:8080" {
		t.Fatalf("edge http addr = %q", cfg.Edge.HTTPAddr)
	}
	if cfg.Edge.DataAddr != "127.0.0.1:8081" {
		t.Fatalf("edge data addr = %q", cfg.Edge.DataAddr)
	}
	if cfg.Connector.EdgeDataURL != "ws://127.0.0.1:8081/_airpc/data" {
		t.Fatalf("connector edge data url = %q", cfg.Connector.EdgeDataURL)
	}
	if got := len(cfg.Routes); got != 4 {
		t.Fatalf("routes = %d", got)
	}

	route := cfg.Routes[0]
	if route.Name != "demo" || route.Mode != ModeHTTP {
		t.Fatalf("first route = %#v", route)
	}
	if route.MaxInlineRequest() != Bytes(4*1024*1024) {
		t.Fatalf("max inline request bytes = %d", route.MaxInlineRequest())
	}
	if route.Timeout.Duration != 30*time.Second {
		t.Fatalf("timeout = %s", route.Timeout.Duration)
	}
}

func TestSubjectDerivation(t *testing.T) {
	route := Route{Name: "demo"}
	if got := route.UnarySubject(); got != "airpc.v1.route.demo.unary" {
		t.Fatalf("UnarySubject() = %q", got)
	}
	if got := route.OpenSubject(); got != "airpc.v1.route.demo.open" {
		t.Fatalf("OpenSubject() = %q", got)
	}
	if got := route.QueueGroup(); got != "airpc.route.demo.connectors" {
		t.Fatalf("QueueGroup() = %q", got)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "missing nats url", yaml: strings.Replace(validConfig, "url: nats://127.0.0.1:4222", "url: ''", 1), want: "nats.url"},
		{name: "bad edge address", yaml: strings.Replace(validConfig, "http_addr: 127.0.0.1:8080", "http_addr: 127.0.0.1", 1), want: "edge.http_addr"},
		{name: "bad connector url", yaml: strings.Replace(validConfig, "ws://127.0.0.1:8081/_airpc/data", "http://127.0.0.1:8081/_airpc/data", 1), want: "connector.edge_data_url"},
		{name: "bad route name", yaml: strings.Replace(validConfig, "name: demo", "name: bad.route", 1), want: "subject"},
		{name: "duplicate route name", yaml: strings.Replace(validConfig, "name: grpc-demo", "name: demo", 1), want: "duplicated"},
		{name: "bad mode", yaml: strings.Replace(validConfig, "mode: http", "mode: smtp", 1), want: "not supported"},
		{name: "bad http target scheme", yaml: strings.Replace(validConfig, "target: http://127.0.0.1:9000", "target: ftp://127.0.0.1:9000", 1), want: "scheme"},
		{name: "bad tcp target", yaml: strings.Replace(validConfig, "target: 127.0.0.1:9001", "target: http://127.0.0.1:9001", 1), want: "host:port"},
		{name: "bad public path", yaml: strings.Replace(validConfig, "public_prefix: /demo", "public_prefix: demo", 1), want: "public_prefix"},
		{name: "bad header", yaml: strings.Replace(validConfig, "forwarded_headers: [accept, content-type, x-request-id]", "forwarded_headers: ['bad header']", 1), want: "forwarded_headers"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

const validConfig = `
nats:
  url: nats://127.0.0.1:4222
edge:
  http_addr: 127.0.0.1:8080
  data_addr: 127.0.0.1:8081
connector:
  edge_data_url: ws://127.0.0.1:8081/_airpc/data
  tunnel_token: dev-token
routes:
  - name: demo
    mode: http
    public_prefix: /demo
    target: http://127.0.0.1:9000
    max_inline_request: 4MiB
    max_inline_response: 16MiB
    timeout: 30s
    forwarded_headers: [accept, content-type, x-request-id]
  - name: echo-tcp
    mode: tcp
    listen: 127.0.0.1:7000
    target: 127.0.0.1:9001
  - name: chat-ws
    mode: websocket
    public_path: /chat
    target: ws://127.0.0.1:9002
  - name: grpc-demo
    mode: grpc
    listen: 127.0.0.1:7001
    target: 127.0.0.1:9003
`
