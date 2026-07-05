package connector

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

type streamRoute struct {
	cfg    config.Route
	target *url.URL
}

type connectorTunnel struct {
	id     string
	conn   *websocket.Conn
	routes map[string]streamRoute

	writeMu  sync.Mutex
	mu       sync.Mutex
	sessions map[string]*connectorSession
}

type connectorSession struct {
	id   string
	recv chan tunnel.Frame
	done chan struct{}
	once sync.Once
}

func (s *connectorSession) close() {
	s.once.Do(func() { close(s.done) })
}

func streamRoutes(routes []config.Route) (map[string]streamRoute, error) {
	out := make(map[string]streamRoute)
	for _, r := range routes {
		if r.Mode == config.ModeHTTP {
			continue
		}
		var target *url.URL
		if r.Mode == config.ModeWebSocket {
			u, err := url.Parse(r.Target)
			if err != nil {
				return nil, fmt.Errorf("parse route %s target: %w", r.Name, err)
			}
			target = u
		}
		out[r.Name] = streamRoute{cfg: r, target: target}
	}
	return out, nil
}

func dialConnectorTunnel(ctx context.Context, cfg config.Config, id string, routes map[string]streamRoute) (*connectorTunnel, error) {
	if len(routes) == 0 {
		return nil, nil
	}
	u, err := url.Parse(cfg.Connector.EdgeDataURL)
	if err != nil {
		return nil, fmt.Errorf("parse edge data url: %w", err)
	}
	query := u.Query()
	query.Set("connector_id", id)
	u.RawQuery = query.Encode()
	requestHeader := http.Header{}
	if cfg.Connector.TunnelToken != "" {
		requestHeader.Set("Authorization", "Bearer "+cfg.Connector.TunnelToken)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, u.String(), requestHeader)
	if err != nil {
		return nil, fmt.Errorf("connect data tunnel: %w", err)
	}
	ct := &connectorTunnel{id: id, conn: conn, routes: routes, sessions: make(map[string]*connectorSession)}
	go ct.run(ctx)
	return ct, nil
}

func (t *connectorTunnel) run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		_ = t.conn.Close()
	}()
	defer t.closeSessions()
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
		if frame.Type == tunnel.FrameOpen {
			t.startOpen(ctx, frame)
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

func (t *connectorTunnel) startOpen(ctx context.Context, frame tunnel.Frame) {
	route, ok := t.routes[frame.Route]
	if !ok || route.cfg.Mode != frame.Kind {
		_ = t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: frame.SessionID, Error: "route is not configured"})
		return
	}
	session := &connectorSession{id: frame.SessionID, recv: make(chan tunnel.Frame, 16), done: make(chan struct{})}
	t.mu.Lock()
	if _, exists := t.sessions[frame.SessionID]; exists {
		t.mu.Unlock()
		_ = t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: frame.SessionID, Error: "session already exists"})
		return
	}
	t.sessions[frame.SessionID] = session
	t.mu.Unlock()

	go func() {
		defer t.removeSession(frame.SessionID)
		switch route.cfg.Mode {
		case config.ModeTCP, config.ModeGRPC:
			t.handleOpaqueTCP(ctx, route, session)
		case config.ModeWebSocket:
			t.handleWebSocket(ctx, route, session, frame.Payload)
		}
	}()
}

func (t *connectorTunnel) handleOpaqueTCP(ctx context.Context, route streamRoute, session *connectorSession) {
	backend, err := (&net.Dialer{}).DialContext(ctx, "tcp", route.cfg.Target)
	if err != nil {
		_ = t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: session.id, Error: "backend dial failed"})
		return
	}
	defer backend.Close()
	defer t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameClose, SessionID: session.id})

	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		buf := make([]byte, 32*1024)
		for {
			n, err := backend.Read(buf)
			if n > 0 {
				frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameData, SessionID: session.id, DataKind: tunnel.DataBinary, Payload: append([]byte(nil), buf[:n]...)}
				if writeErr := t.write(frame); writeErr != nil {
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
		case <-backendDone:
			return
		case <-session.done:
			return
		case frame := <-session.recv:
			switch frame.Type {
			case tunnel.FrameData:
				if _, err := backend.Write(frame.Payload); err != nil {
					return
				}
			case tunnel.FrameClose, tunnel.FrameError:
				return
			}
		}
	}
}

func (t *connectorTunnel) handleWebSocket(ctx context.Context, route streamRoute, session *connectorSession, payload []byte) {
	path := string(payload)
	if path == "" {
		path = "/"
	}
	target, err := targetURL(route.target, path)
	if err != nil {
		_ = t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: session.id, Error: "invalid websocket path"})
		return
	}
	backend, _, err := websocket.DefaultDialer.DialContext(ctx, target.String(), http.Header{})
	if err != nil {
		_ = t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: session.id, Error: "backend websocket dial failed"})
		return
	}
	defer backend.Close()
	defer t.write(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameClose, SessionID: session.id})

	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		for {
			messageType, payload, err := backend.ReadMessage()
			if err != nil {
				return
			}
			dataKind := tunnel.DataBinary
			if messageType == websocket.TextMessage {
				dataKind = tunnel.DataText
			} else if messageType != websocket.BinaryMessage {
				continue
			}
			frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameData, SessionID: session.id, DataKind: dataKind, Payload: payload}
			if err := t.write(frame); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-backendDone:
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
				if err := backend.WriteMessage(messageType, frame.Payload); err != nil {
					return
				}
			case tunnel.FrameClose, tunnel.FrameError:
				return
			}
		}
	}
}

func (t *connectorTunnel) write(frame tunnel.Frame) error {
	data, err := tunnel.EncodeFrame(frame)
	if err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (t *connectorTunnel) removeSession(sessionID string) {
	t.mu.Lock()
	session := t.sessions[sessionID]
	delete(t.sessions, sessionID)
	t.mu.Unlock()
	if session != nil {
		session.close()
	}
}

func (t *connectorTunnel) closeSessions() {
	t.mu.Lock()
	sessions := t.sessions
	t.sessions = make(map[string]*connectorSession)
	t.mu.Unlock()
	for _, session := range sessions {
		session.close()
	}
}
