// Package datatunnel implements the runtime for the connector-owned data
// WebSocket: one Link multiplexes many Sessions, each relaying one public
// connection to one private backend. Both edge and connector use the same
// Link, Session, and relay loops; only session setup differs per side.
package datatunnel

import (
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

// Link wraps one data-tunnel WebSocket connection and dispatches frames to
// sessions. onOpen, when non-nil, is called synchronously from the read loop
// for every FrameOpen (the connector side accepts sessions there); it must
// register the session before returning so that data frames arriving right
// after the open find it.
type Link struct {
	conn   *websocket.Conn
	onOpen func(tunnel.Frame)

	writeMu  sync.Mutex
	mu       sync.Mutex
	sessions map[string]*Session
}

func NewLink(conn *websocket.Conn, onOpen func(tunnel.Frame)) *Link {
	return &Link{conn: conn, onOpen: onOpen, sessions: make(map[string]*Session)}
}

// Run reads and dispatches frames until the connection fails, then closes all
// sessions. It must be the only reader of the connection.
func (l *Link) Run() {
	defer l.Close()
	for {
		messageType, data, err := l.conn.ReadMessage()
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
			if l.onOpen != nil {
				l.onOpen(frame)
			}
			continue
		}
		l.dispatch(frame)
	}
}

func (l *Link) dispatch(frame tunnel.Frame) {
	l.mu.Lock()
	session := l.sessions[frame.SessionID]
	l.mu.Unlock()
	if session == nil {
		return
	}
	if frame.Type == tunnel.FrameWindow {
		session.addSendCredit(frame.Window)
		return
	}
	select {
	case session.recv <- frame:
	case <-session.done:
	default:
		// The peer overran its send window: a protocol violation, since recv
		// is sized for a full window plus control frames. Kill the session
		// rather than block the whole link or silently drop stream bytes.
		_ = l.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: frame.SessionID, Error: "flow control violation"})
		l.removeSession(frame.SessionID)
	}
}

// Open registers a session and sends its FrameOpen (edge side).
func (l *Link) Open(frame tunnel.Frame) (*Session, error) {
	session, err := l.addSession(frame.SessionID)
	if err != nil {
		return nil, err
	}
	if err := l.WriteFrame(frame); err != nil {
		l.removeSession(frame.SessionID)
		return nil, err
	}
	return session, nil
}

// Accept registers a session for a received FrameOpen (connector side, and
// the edge side for connector-initiated http-stream sessions).
func (l *Link) Accept(sessionID string) (*Session, error) {
	return l.addSession(sessionID)
}

// Session returns the registered session with the given ID, or nil.
func (l *Link) Session(sessionID string) *Session {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sessions[sessionID]
}

func (l *Link) addSession(sessionID string) (*Session, error) {
	session := newSession(sessionID, l)
	l.mu.Lock()
	if _, exists := l.sessions[sessionID]; exists {
		l.mu.Unlock()
		return nil, fmt.Errorf("session %s already exists", sessionID)
	}
	l.sessions[sessionID] = session
	l.mu.Unlock()
	return session, nil
}

func (l *Link) removeSession(sessionID string) {
	l.mu.Lock()
	session := l.sessions[sessionID]
	delete(l.sessions, sessionID)
	l.mu.Unlock()
	if session != nil {
		session.markDone()
	}
}

func (l *Link) WriteFrame(frame tunnel.Frame) error {
	data, err := tunnel.EncodeFrame(frame)
	if err != nil {
		return err
	}
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	return l.conn.WriteMessage(websocket.BinaryMessage, data)
}

// Close tears down the connection and every session.
func (l *Link) Close() {
	_ = l.conn.Close()
	l.mu.Lock()
	sessions := l.sessions
	l.sessions = make(map[string]*Session)
	l.mu.Unlock()
	for _, session := range sessions {
		session.markDone()
	}
}
