package edge

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/datatunnel"
	"github.com/terion-name/airpc/internal/grpcobs"
	"github.com/terion-name/airpc/internal/natscore"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
	"github.com/terion-name/airpc/internal/telemetry"
)

const dataTunnelPath = "/_airpc/data"

// tunnelRegistry tracks the active data-tunnel link per connector ID.
type tunnelRegistry struct {
	mu    sync.RWMutex
	links map[string]*datatunnel.Link
}

func newTunnelRegistry() *tunnelRegistry {
	return &tunnelRegistry{links: make(map[string]*datatunnel.Link)}
}

func (r *tunnelRegistry) register(connectorID string, link *datatunnel.Link) {
	r.mu.Lock()
	old := r.links[connectorID]
	r.links[connectorID] = link
	r.mu.Unlock()
	if old != nil {
		old.Close()
	}
}

func (r *tunnelRegistry) unregister(connectorID string, link *datatunnel.Link) {
	r.mu.Lock()
	if r.links[connectorID] == link {
		delete(r.links, connectorID)
	}
	r.mu.Unlock()
}

func (r *tunnelRegistry) get(connectorID string) *datatunnel.Link {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.links[connectorID]
}

// closeAll drops every tunnel. HTTP server shutdown does not close hijacked
// WebSocket connections, so edge shutdown must do it here for connectors to
// notice and redial.
func (r *tunnelRegistry) closeAll() {
	r.mu.Lock()
	links := r.links
	r.links = make(map[string]*datatunnel.Link)
	r.mu.Unlock()
	for _, link := range links {
		link.Close()
	}
}

type dataHandler struct {
	registry *tunnelRegistry
	token    string
}

func (h dataHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != dataTunnelPath {
		http.NotFound(w, req)
		return
	}
	connectorID := req.URL.Query().Get("connector_id")
	if connectorID == "" {
		connectorID = req.URL.Query().Get("id")
	}
	if err := config.ValidateSubjectToken("connector_id", connectorID); err != nil {
		http.Error(w, "invalid connector id", http.StatusBadRequest)
		return
	}
	if h.token != "" && req.URL.Query().Get("token") != h.token && req.Header.Get("Authorization") != "Bearer "+h.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	var link *datatunnel.Link
	link = datatunnel.NewLink(conn, func(frame tunnel.Frame) { acceptStreamOpen(link, frame) })
	h.registry.register(connectorID, link)
	link.Run()
	h.registry.unregister(connectorID, link)
}

// streamClaimTimeout bounds how long a connector-initiated http-stream
// session may sit unclaimed (its NATS reply lost or the unary handler gone)
// before the edge discards it.
const streamClaimTimeout = 10 * time.Second

// acceptStreamOpen registers connector-initiated response-stream sessions so
// the unary handler that receives the matching NATS reply can claim them.
func acceptStreamOpen(link *datatunnel.Link, frame tunnel.Frame) {
	if frame.Kind != tunnel.KindHTTPStream {
		return
	}
	session, err := link.Accept(frame.SessionID)
	if err != nil {
		return
	}
	time.AfterFunc(streamClaimTimeout, func() {
		if !session.Claimed() {
			session.CloseWithError("response stream was not claimed")
		}
	})
}

// claimStreamSession waits briefly for the http-stream FrameOpen to arrive
// (it races the NATS reply) and claims the session for the calling handler.
func claimStreamSession(link *datatunnel.Link, sessionID string) *datatunnel.Session {
	deadline := time.Now().Add(2 * time.Second)
	for {
		if session := link.Session(sessionID); session != nil {
			if session.TryClaim() {
				return session
			}
			return nil
		}
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tcpRoutes(routes []config.Route) []config.Route {
	out := make([]config.Route, 0, len(routes))
	for _, r := range routes {
		if r.Mode == config.ModeTCP || r.Mode == config.ModeGRPC {
			out = append(out, r)
		}
	}
	return out
}

func wsRoutes(routes []config.Route) []route {
	out := make([]route, 0, len(routes))
	for _, r := range routes {
		if r.Mode == config.ModeWebSocket {
			out = append(out, route{cfg: r, prefix: r.PublicPrefix, exact: r.PublicPath})
		}
	}
	return sortRoutes(out)
}

// errTunnelUnavailable marks transient open failures: the drawn connector's
// data tunnel is down, or its tunnel dropped between accept and open. Another
// queue member may succeed, so these are retried until the open timeout.
var errTunnelUnavailable = errors.New(tunnel.ErrTunnelNotConnected)

// openDataSession selects a connector for the route over NATS, then opens a
// session on that connector's data tunnel, retrying transient tunnel-down
// rejections until the route timeout expires.
func openDataSession(ctx context.Context, nc *natscore.Client, registry *tunnelRegistry, r config.Route, sessionID, path string) (*datatunnel.Session, error) {
	openCtx, cancel := context.WithTimeout(ctx, routeTimeout(r))
	defer cancel()
	for {
		session, err := attemptOpenDataSession(openCtx, nc, registry, r, sessionID, path)
		if err == nil {
			return session, nil
		}
		if !errors.Is(err, errTunnelUnavailable) {
			return nil, err
		}
		select {
		case <-openCtx.Done():
			return nil, err
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func attemptOpenDataSession(openCtx context.Context, nc *natscore.Client, registry *tunnelRegistry, r config.Route, sessionID, path string) (*datatunnel.Session, error) {
	deadline, _ := openCtx.Deadline()
	req := tunnel.OpenRequest{
		Version:        tunnel.Version,
		RequestID:      sessionID,
		SessionID:      sessionID,
		Route:          r.Name,
		Kind:           r.Mode,
		Path:           path,
		DeadlineUnixMS: deadline.UnixMilli(),
	}
	payload, err := tunnel.EncodeOpenRequest(req)
	if err != nil {
		return nil, err
	}
	reply, err := nc.Request(openCtx, r.OpenSubject(), payload)
	if err != nil {
		return nil, err
	}
	resp, err := tunnel.DecodeOpenResponse(reply)
	if err != nil {
		return nil, err
	}
	if resp.RequestID != sessionID || !resp.Accepted {
		if resp.Error == tunnel.ErrTunnelNotConnected {
			return nil, errTunnelUnavailable
		}
		if resp.Error == "" {
			resp.Error = "open rejected"
		}
		return nil, errors.New(resp.Error)
	}
	link := registry.get(resp.ConnectorID)
	if link == nil {
		return nil, fmt.Errorf("connector %s has no registered data tunnel: %w", resp.ConnectorID, errTunnelUnavailable)
	}
	frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameOpen, SessionID: sessionID, Route: r.Name, Kind: r.Mode, Payload: []byte(path)}
	return link.Open(frame)
}

func serveTCPRoute(ctx context.Context, nc *natscore.Client, registry *tunnelRegistry, r config.Route, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go handleTCPConn(ctx, nc, registry, r, conn)
	}
}

func handleTCPConn(ctx context.Context, nc *natscore.Client, registry *tunnelRegistry, r config.Route, conn net.Conn) {
	sessionID, err := requestID()
	if err != nil {
		_ = conn.Close()
		return
	}
	session, err := openDataSession(ctx, nc, registry, r, sessionID, "/")
	if err != nil {
		_ = conn.Close()
		return
	}
	telemetry.StreamSessionsTotal.WithLabelValues(r.Name, r.Mode).Inc()
	active := telemetry.StreamSessionsActive.WithLabelValues(r.Name, r.Mode)
	active.Inc()
	defer active.Dec()

	observed := observeConn(conn, r)
	defer observed.finish()
	datatunnel.RelayConn(ctx, session, observed, r.IdleTimeout.Duration)
}

// observedConn counts relayed bytes and, on grpc routes, feeds a passive
// gRPC observer. It must keep exposing CloseWrite so half-close propagation
// still reaches the wrapped connection.
type observedConn struct {
	net.Conn
	in, out  prometheus.Counter
	observer *grpcobs.Observer // nil on non-grpc routes
}

func observeConn(conn net.Conn, r config.Route) *observedConn {
	o := &observedConn{
		Conn: conn,
		in:   telemetry.StreamBytes.WithLabelValues(r.Name, "in"),
		out:  telemetry.StreamBytes.WithLabelValues(r.Name, "out"),
	}
	if r.Mode == config.ModeGRPC {
		o.observer = grpcobs.New(r.Name)
	}
	return o
}

func (c *observedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.in.Add(float64(n))
		if c.observer != nil {
			c.observer.ClientBytes(p[:n])
		}
	}
	return n, err
}

func (c *observedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.out.Add(float64(n))
		if c.observer != nil {
			c.observer.BackendBytes(p[:n])
		}
	}
	return n, err
}

func (c *observedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

func (c *observedConn) finish() {
	if c.observer != nil {
		c.observer.Close()
	}
}

func handleWebSocketRoute(ctx context.Context, nc *natscore.Client, registry *tunnelRegistry, r route, suffix string, w http.ResponseWriter, req *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	client, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}

	sessionID, err := requestID()
	if err != nil {
		_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "session id"), time.Now().Add(time.Second))
		_ = client.Close()
		return
	}
	session, err := openDataSession(req.Context(), nc, registry, r.cfg, sessionID, suffix)
	if err != nil {
		_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "connector unavailable"), time.Now().Add(time.Second))
		_ = client.Close()
		return
	}
	telemetry.StreamSessionsTotal.WithLabelValues(r.cfg.Name, r.cfg.Mode).Inc()
	active := telemetry.StreamSessionsActive.WithLabelValues(r.cfg.Name, r.cfg.Mode)
	active.Inc()
	defer active.Dec()
	datatunnel.RelayWebSocket(ctx, session, client, r.cfg.IdleTimeout.Duration)
}

func closeListeners(listeners []net.Listener) {
	for _, listener := range listeners {
		_ = listener.Close()
	}
}

func closeHTTPServer(ctx context.Context, server *http.Server) {
	if server != nil {
		_ = server.Shutdown(ctx)
	}
}
