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
	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/natscore"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

const dataTunnelPath = "/_airpc/data"

type tunnelRegistry struct {
	mu      sync.RWMutex
	tunnels map[string]*edgeTunnel
}

func newTunnelRegistry() *tunnelRegistry {
	return &tunnelRegistry{tunnels: make(map[string]*edgeTunnel)}
}

func (r *tunnelRegistry) register(connectorID string, t *edgeTunnel) {
	r.mu.Lock()
	old := r.tunnels[connectorID]
	r.tunnels[connectorID] = t
	r.mu.Unlock()
	if old != nil {
		old.close()
	}
}

func (r *tunnelRegistry) unregister(connectorID string, t *edgeTunnel) {
	r.mu.Lock()
	if r.tunnels[connectorID] == t {
		delete(r.tunnels, connectorID)
	}
	r.mu.Unlock()
}

func (r *tunnelRegistry) get(connectorID string) *edgeTunnel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tunnels[connectorID]
}

type edgeTunnel struct {
	connectorID string
	conn        *websocket.Conn
	registry    *tunnelRegistry

	writeMu  sync.Mutex
	mu       sync.Mutex
	sessions map[string]*edgeSession
}

type edgeSession struct {
	id   string
	recv chan tunnel.Frame
	done chan struct{}
	once sync.Once
}

func newEdgeTunnel(connectorID string, conn *websocket.Conn, registry *tunnelRegistry) *edgeTunnel {
	return &edgeTunnel{connectorID: connectorID, conn: conn, registry: registry, sessions: make(map[string]*edgeSession)}
}

func (t *edgeTunnel) run() {
	defer func() {
		t.registry.unregister(t.connectorID, t)
		t.closeSessions()
		_ = t.conn.Close()
	}()
	for {
		messageType, data, err := t.conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		frame, err := tunnel.DecodeFrame(data)
		if err != nil {
			continue
		}
		t.mu.Lock()
		session := t.sessions[frame.SessionID]
		t.mu.Unlock()
		if session != nil {
			select {
			case session.recv <- frame:
			case <-session.done:
			default:
			}
		}
	}
}

func (t *edgeTunnel) openSession(frame tunnel.Frame) (*edgeSession, error) {
	session := &edgeSession{id: frame.SessionID, recv: make(chan tunnel.Frame, 16), done: make(chan struct{})}
	t.mu.Lock()
	if _, exists := t.sessions[frame.SessionID]; exists {
		t.mu.Unlock()
		return nil, fmt.Errorf("session %s already exists", frame.SessionID)
	}
	t.sessions[frame.SessionID] = session
	t.mu.Unlock()
	if err := t.write(frame); err != nil {
		t.removeSession(frame.SessionID)
		return nil, err
	}
	return session, nil
}

func (t *edgeTunnel) write(frame tunnel.Frame) error {
	data, err := tunnel.EncodeFrame(frame)
	if err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (t *edgeTunnel) removeSession(sessionID string) {
	t.mu.Lock()
	session := t.sessions[sessionID]
	delete(t.sessions, sessionID)
	t.mu.Unlock()
	if session != nil {
		session.close()
	}
}

func (t *edgeTunnel) closeSessions() {
	t.mu.Lock()
	sessions := t.sessions
	t.sessions = make(map[string]*edgeSession)
	t.mu.Unlock()
	for _, session := range sessions {
		session.close()
	}
}

func (t *edgeTunnel) close() {
	_ = t.conn.Close()
	t.closeSessions()
}

func (s *edgeSession) close() {
	s.once.Do(func() { close(s.done) })
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
	t := newEdgeTunnel(connectorID, conn, h.registry)
	h.registry.register(connectorID, t)
	t.run()
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

func openDataSession(ctx context.Context, nc *natscore.Client, registry *tunnelRegistry, r config.Route, sessionID, path string) (*edgeTunnel, *edgeSession, error) {
	timeout := routeTimeout(r)
	openCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
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
		return nil, nil, err
	}
	reply, err := nc.Request(openCtx, r.OpenSubject(), payload)
	if err != nil {
		return nil, nil, err
	}
	resp, err := tunnel.DecodeOpenResponse(reply)
	if err != nil {
		return nil, nil, err
	}
	if resp.RequestID != sessionID || !resp.Accepted {
		if resp.Error == "" {
			resp.Error = "open rejected"
		}
		return nil, nil, errors.New(resp.Error)
	}
	edgeTunnel := registry.get(resp.ConnectorID)
	if edgeTunnel == nil {
		return nil, nil, fmt.Errorf("connector %s has no active data tunnel", resp.ConnectorID)
	}
	frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameOpen, SessionID: sessionID, Route: r.Name, Kind: r.Mode, Payload: []byte(path)}
	session, err := edgeTunnel.openSession(frame)
	if err != nil {
		return nil, nil, err
	}
	return edgeTunnel, session, nil
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
	defer conn.Close()
	sessionID, err := requestID()
	if err != nil {
		return
	}
	edgeTunnel, session, err := openDataSession(ctx, nc, registry, r, sessionID, "/")
	if err != nil {
		return
	}
	defer edgeTunnel.removeSession(sessionID)
	defer edgeTunnel.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameClose, SessionID: sessionID})

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameData, SessionID: sessionID, DataKind: tunnel.DataBinary, Payload: append([]byte(nil), buf[:n]...)}
				if writeErr := edgeTunnel.write(frame); writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-clientDone:
			return
		case <-session.done:
			return
		case frame := <-session.recv:
			switch frame.Type {
			case tunnel.FrameData:
				if _, err := conn.Write(frame.Payload); err != nil {
					return
				}
			case tunnel.FrameClose, tunnel.FrameError:
				return
			}
		}
	}
}

func handleWebSocketRoute(ctx context.Context, nc *natscore.Client, registry *tunnelRegistry, r route, suffix string, w http.ResponseWriter, req *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	client, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	defer client.Close()

	sessionID, err := requestID()
	if err != nil {
		_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "session id"), time.Now().Add(time.Second))
		return
	}
	edgeTunnel, session, err := openDataSession(req.Context(), nc, registry, r.cfg, sessionID, suffix)
	if err != nil {
		_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "connector unavailable"), time.Now().Add(time.Second))
		return
	}
	defer edgeTunnel.removeSession(sessionID)
	defer edgeTunnel.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameClose, SessionID: sessionID})

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		for {
			messageType, payload, err := client.ReadMessage()
			if err != nil {
				return
			}
			dataKind := tunnel.DataBinary
			if messageType == websocket.TextMessage {
				dataKind = tunnel.DataText
			} else if messageType != websocket.BinaryMessage {
				continue
			}
			frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameData, SessionID: sessionID, DataKind: dataKind, Payload: payload}
			if err := edgeTunnel.write(frame); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-clientDone:
			return
		case <-session.done:
			return
		case frame := <-session.recv:
			switch frame.Type {
			case tunnel.FrameData:
				messageType := websocket.BinaryMessage
				if frame.DataKind == tunnel.DataText {
					messageType = websocket.TextMessage
				}
				if err := client.WriteMessage(messageType, frame.Payload); err != nil {
					return
				}
			case tunnel.FrameClose, tunnel.FrameError:
				return
			}
		}
	}
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
