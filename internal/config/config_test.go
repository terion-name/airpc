package config

import (
	"os"
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
	if cfg.NATS.URLs[0] != "nats://127.0.0.1:4222" {
		t.Fatalf("nats urls = %#v", cfg.NATS.URLs)
	}
	if cfg.Connector.ID != "local-1" {
		t.Fatalf("connector id = %q", cfg.Connector.ID)
	}
	if cfg.Edge.HTTP.Listen != "127.0.0.1:8080" {
		t.Fatalf("edge listen = %q", cfg.Edge.HTTP.Listen)
	}
	if got := len(cfg.Edge.Routes); got != 4 {
		t.Fatalf("edge routes = %d", got)
	}
	r, ok := cfg.ConnectorRoute("demo")
	if !ok {
		t.Fatalf("demo connector route missing")
	}
	if got := r.RequestTimeout.Duration; got != 30*time.Second {
		t.Fatalf("route timeout = %s", got)
	}
	if got := r.MaxRequestBytes.Bytes; got != 4*1024*1024 {
		t.Fatalf("route max request bytes = %d", got)
	}
	if u, err := r.TargetURL(); err != nil || u.Scheme != "http" {
		t.Fatalf("TargetURL() = %v, %v", u, err)
	}
	if !cfg.ConnectorHasRoute("echo-tcp") {
		t.Fatalf("expected connector to have echo-tcp")
	}
}

func TestValidateRejectsBadPlanConfig(t *testing.T) {
	tests := []struct {
		name string
		repl func(string) string
		want string
	}{
		{
			name: "bad route token wildcard",
			repl: func(s string) string { return strings.Replace(s, "name: demo", "name: bad*route", 1) },
			want: "wildcards",
		},
		{
			name: "bad connector id",
			repl: func(s string) string { return strings.Replace(s, "id: local-1", "id: bad>id", 1) },
			want: "connector.id",
		},
		{
			name: "duplicate edge route name",
			repl: func(s string) string { return strings.Replace(s, "name: echo-tcp", "name: demo", 1) },
			want: "duplicated",
		},
		{
			name: "duplicate connector route name",
			repl: func(s string) string {
				return strings.Replace(s, "name: echo-tcp\n      type: tcp\n      target", "name: demo\n      type: tcp\n      target", 1)
			},
			want: "duplicated",
		},
		{
			name: "invalid route type",
			repl: func(s string) string { return strings.Replace(s, "type: http", "type: smtp", 1) },
			want: "not supported",
		},
		{
			name: "bad http target scheme",
			repl: func(s string) string {
				return strings.Replace(s, "target: http://127.0.0.1:8081", "target: ftp://127.0.0.1:8081", 1)
			},
			want: "scheme",
		},
		{
			name: "target rejects credentials",
			repl: func(s string) string {
				return strings.Replace(s, "target: http://127.0.0.1:8081", "target: http://user:pass@127.0.0.1:8081", 1)
			},
			want: "credentials",
		},
		{
			name: "bad tcp target",
			repl: func(s string) string {
				return strings.Replace(s, "target: 127.0.0.1:7000", "target: http://127.0.0.1:7000", 1)
			},
			want: "host:port",
		},
		{
			name: "bad edge listen",
			repl: func(s string) string { return strings.Replace(s, "listen: 127.0.0.1:9000", "listen: 127.0.0.1", 1) },
			want: "listen",
		},
		{
			name: "bad tunnel URL scheme",
			repl: func(s string) string {
				return strings.Replace(s, "edge_tunnel_url: ws://127.0.0.1:8080/_airpc/v1/tunnel", "edge_tunnel_url: http://127.0.0.1:8080/_airpc/v1/tunnel", 1)
			},
			want: "scheme",
		},
		{
			name: "route type mismatch",
			repl: func(s string) string {
				return strings.Replace(s, "name: demo\n      type: http\n      target: http://127.0.0.1:8081", "name: demo\n      type: tcp\n      target: 127.0.0.1:8081", 1)
			},
			want: "does not match",
		},
		{
			name: "invalid duration",
			repl: func(s string) string { return strings.Replace(s, "request_timeout: 30s", "request_timeout: 0s", 1) },
			want: "must be positive",
		},
		{
			name: "invalid size",
			repl: func(s string) string { return strings.Replace(s, "max_request_bytes: 4MiB", "max_request_bytes: 0", 1) },
			want: "must be positive",
		},
	}
	base := mustReadExample(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.repl(base)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestConnectorIDOverrideSemantics(t *testing.T) {
	base := strings.Replace(mustReadExample(t), "id: local-1", "id: ''", 1)
	path := filepath.Join(t.TempDir(), "airpc.yaml")
	if err := os.WriteFile(path, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "connector.id") {
		t.Fatalf("LoadFile without connector id error = %v", err)
	}
	cfg, err := LoadFileWithConnectorID(path, "override-1")
	if err != nil {
		t.Fatalf("LoadFileWithConnectorID: %v", err)
	}
	if cfg.Connector.ID != "override-1" {
		t.Fatalf("connector id = %q", cfg.Connector.ID)
	}
	if _, err := LoadFileWithConnectorID(path, "bad id"); err == nil || !strings.Contains(err.Error(), "connector.id") {
		t.Fatalf("bad override error = %v", err)
	}
}

func TestParseSize(t *testing.T) {
	tests := map[string]int64{"10": 10, "1KiB": 1024, "2MB": 2_000_000, "3mib": 3 * 1024 * 1024}
	for raw, want := range tests {
		got, err := ParseSize(raw)
		if err != nil {
			t.Fatalf("ParseSize(%q): %v", raw, err)
		}
		if got != want {
			t.Fatalf("ParseSize(%q) = %d, want %d", raw, got, want)
		}
	}
}

func mustReadExample(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "airpc.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
