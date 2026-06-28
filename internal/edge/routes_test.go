package edge

import (
	"testing"

	"github.com/terion-name/airpc/internal/config"
)

func TestRouteModeSelectors(t *testing.T) {
	routes := []config.Route{
		{Name: "http", Mode: config.ModeHTTP, PublicPrefix: "/h"},
		{Name: "ws", Mode: config.ModeWebSocket, PublicPath: "/ws"},
		{Name: "tcp", Mode: config.ModeTCP, Listen: "127.0.0.1:7000"},
		{Name: "grpc", Mode: config.ModeGRPC, Listen: "127.0.0.1:7001"},
	}
	if got := len(httpRoutes(routes)); got != 1 {
		t.Fatalf("httpRoutes = %d", got)
	}
	if got := len(wsRoutes(routes)); got != 1 {
		t.Fatalf("wsRoutes = %d", got)
	}
	if got := len(tcpRoutes(routes)); got != 2 {
		t.Fatalf("tcpRoutes = %d", got)
	}
}
