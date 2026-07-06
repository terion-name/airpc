package datatunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

const relayBufferSize = 32 * 1024

// RelayConn pipes conn ⇄ session until both directions have finished, the
// session or ctx ends, or the connection idles longer than idleTimeout
// (0 disables the idle check). TCP half-closes are propagated: a local read
// EOF becomes a tunnel EOF frame, and a peer EOF half-closes conn while the
// opposite direction keeps flowing.
func RelayConn(ctx context.Context, session *Session, conn net.Conn, idleTimeout time.Duration) {
	defer session.Close()
	defer conn.Close()

	idle := newIdleGuard(idleTimeout)
	defer idle.stop()

	readResult := make(chan error, 1)
	go func() {
		buf := make([]byte, relayBufferSize)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if sendErr := session.SendData(tunnel.DataBinary, buf[:n]); sendErr != nil {
					readResult <- sendErr
					return
				}
				idle.touch()
			}
			if err != nil {
				readResult <- err
				return
			}
		}
	}()

	localEOF, remoteEOF := false, false
	for {
		select {
		case <-ctx.Done():
			return
		case <-session.Done():
			return
		case err := <-readResult:
			readResult = nil
			if !errors.Is(err, io.EOF) {
				return
			}
			localEOF = true
			if session.SendEOF() != nil || remoteEOF {
				return
			}
		case frame := <-session.Recv():
			switch frame.Type {
			case tunnel.FrameData:
				if _, err := conn.Write(frame.Payload); err != nil {
					return
				}
				idle.touch()
				session.AckData()
			case tunnel.FrameEOF:
				remoteEOF = true
				closeWrite(conn)
				if localEOF {
					return
				}
			case tunnel.FrameClose, tunnel.FrameError:
				return
			}
		case <-idle.c:
			if idle.expired() {
				return
			}
		}
	}
}

// RelayWebSocket pipes ws ⇄ session as discrete messages until either side
// closes, the session or ctx ends, or the connection idles longer than
// idleTimeout (0 disables). Ping/pong control frames are answered by the
// transport, are not relayed, and do not count as activity.
func RelayWebSocket(ctx context.Context, session *Session, ws *websocket.Conn, idleTimeout time.Duration) {
	defer session.Close()
	defer ws.Close()

	idle := newIdleGuard(idleTimeout)
	defer idle.stop()

	readResult := make(chan error, 1)
	go func() {
		for {
			messageType, payload, err := ws.ReadMessage()
			if err != nil {
				readResult <- err
				return
			}
			dataKind := tunnel.DataBinary
			if messageType == websocket.TextMessage {
				dataKind = tunnel.DataText
			} else if messageType != websocket.BinaryMessage {
				continue
			}
			if sendErr := session.SendData(dataKind, payload); sendErr != nil {
				readResult <- sendErr
				return
			}
			idle.touch()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-session.Done():
			return
		case err := <-readResult:
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) {
				session.CloseWithCode(closeErr.Code, closeErr.Text)
			}
			return
		case frame := <-session.Recv():
			switch frame.Type {
			case tunnel.FrameData:
				messageType := websocket.BinaryMessage
				if frame.DataKind == tunnel.DataText {
					messageType = websocket.TextMessage
				}
				if err := ws.WriteMessage(messageType, frame.Payload); err != nil {
					return
				}
				idle.touch()
				session.AckData()
			case tunnel.FrameClose:
				if frame.CloseCode != 0 {
					message := websocket.FormatCloseMessage(frame.CloseCode, string(frame.Payload))
					_ = ws.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
				}
				return
			case tunnel.FrameEOF, tunnel.FrameError:
				return
			}
		case <-idle.c:
			if idle.expired() {
				return
			}
		}
	}
}

func closeWrite(conn net.Conn) {
	type writeCloser interface{ CloseWrite() error }
	if wc, ok := conn.(writeCloser); ok {
		_ = wc.CloseWrite()
	}
}

// idleGuard tracks last activity across the relay goroutines. Its timer
// channel c is nil when the guard is disabled, so it never fires in a select.
type idleGuard struct {
	timeout time.Duration
	last    atomic.Int64 // unix nanos of last activity
	timer   *time.Timer
	c       <-chan time.Time
}

func newIdleGuard(timeout time.Duration) *idleGuard {
	g := &idleGuard{timeout: timeout}
	g.last.Store(time.Now().UnixNano())
	if timeout > 0 {
		g.timer = time.NewTimer(timeout)
		g.c = g.timer.C
	}
	return g
}

func (g *idleGuard) touch() { g.last.Store(time.Now().UnixNano()) }

// expired reports whether the idle deadline has passed; if not it re-arms the
// timer for the remaining time.
func (g *idleGuard) expired() bool {
	remaining := g.timeout - time.Since(time.Unix(0, g.last.Load()))
	if remaining <= 0 {
		return true
	}
	g.timer.Reset(remaining)
	return false
}

func (g *idleGuard) stop() {
	if g.timer != nil {
		g.timer.Stop()
	}
}
