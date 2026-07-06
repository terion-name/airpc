// Package telemetry defines airpc's Prometheus metrics and the optional
// /metrics listener. Bodies, header values, and tokens are never used as
// label values; labels are limited to route names, connector IDs, gRPC
// methods, and status codes.
package telemetry

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airpc_http_requests_total",
		Help: "HTTP unary requests handled by the edge, by route and response status code.",
	}, []string{"route", "status"})
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "airpc_http_request_duration_seconds",
		Help:    "HTTP unary request duration at the edge, headers to last byte.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})
	StreamSessionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airpc_stream_sessions_total",
		Help: "Stream sessions opened at the edge, by route and mode.",
	}, []string{"route", "mode"})
	StreamSessionsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airpc_stream_sessions_active",
		Help: "Currently relaying stream sessions at the edge, by route and mode.",
	}, []string{"route", "mode"})
	StreamBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airpc_stream_bytes_total",
		Help: "Bytes relayed on tcp/grpc routes; direction is in (public client to backend) or out.",
	}, []string{"route", "direction"})
	TunnelConnected = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airpc_tunnel_connected",
		Help: "Whether the connector's data tunnel to the edge is up (1) or down (0).",
	}, []string{"connector_id"})
	TunnelDials = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airpc_tunnel_dials_total",
		Help: "Data tunnel dial attempts by the connector, by outcome (connected, failed).",
	}, []string{"connector_id", "outcome"})
	GRPCRPCs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "airpc_grpc_rpcs_total",
		Help: "Completed RPCs observed on grpc routes, by method and grpc-status code.",
	}, []string{"route", "method", "code"})
	GRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "airpc_grpc_rpc_duration_seconds",
		Help:    "Observed RPC duration on grpc routes, request headers to trailers.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})
)

// StartServer serves Prometheus metrics on addr at /metrics until ctx is
// done. An empty addr disables the listener; the bound address is returned
// otherwise.
func StartServer(ctx context.Context, addr string) (string, error) {
	if addr == "" {
		return "", nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen metrics %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return listener.Addr().String(), nil
}
