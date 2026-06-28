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
	if cfg.Connector.ID != "dev-connector-1" {
		t.Fatalf("connector id = %q", cfg.Connector.ID)
	}
	if got := cfg.Routes[0].RequestTimeout.Duration; got != 30*time.Second {
		t.Fatalf("route timeout = %s", got)
	}
	if got := cfg.Routes[0].MaxRequestBytes.Bytes; got != 4*1024*1024 {
		t.Fatalf("route max request bytes = %d", got)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name string
		repl func(string) string
		want string
	}{
		{
			name: "bad route name",
			repl: func(s string) string { return strings.Replace(s, "name: demo", "name: Demo", 1) },
			want: "route name",
		},
		{
			name: "bad connector id",
			repl: func(s string) string { return strings.Replace(s, "id: dev-connector-1", "id: -bad", 1) },
			want: "connector.id",
		},
		{
			name: "duplicate route name",
			repl: func(s string) string {
				return s + "\n  - name: demo\n    target: http://127.0.0.1:8081\n    connectors: [dev-connector-1]\n"
			},
			want: "duplicated",
		},
		{
			name: "bad target scheme",
			repl: func(s string) string {
				return strings.Replace(s, "target: http://127.0.0.1:8080", "target: ftp://127.0.0.1", 1)
			},
			want: "scheme",
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
