package datatunnel

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

const (
	// sendWindow is the maximum number of unacknowledged data frames each
	// side may have in flight per session. With relay reads capped at 32 KiB
	// per frame this bounds per-session buffering to ~1 MiB per direction.
	sendWindow = 32
	// recvSlack is headroom in the receive queue for control frames (open
	// echo, EOF, close, error), which do not consume window credit.
	recvSlack = 8
	// ackThreshold batches window updates: credit is returned after
	// consuming this many data frames instead of one update per frame.
	ackThreshold = sendWindow / 2
)

var ErrSessionClosed = errors.New("tunnel session closed")

// Session is one relayed stream inside a Link. The sender blocks in SendData
// once sendWindow data frames are unacknowledged, so one slow session applies
// backpressure to its own peer without stalling other sessions on the link.
type Session struct {
	id   string
	link *Link
	recv chan tunnel.Frame
	done chan struct{}
	once sync.Once

	credit chan struct{}
	// consumed counts data frames processed since the last window update.
	// Only the session's single consuming goroutine touches it (via AckData).
	consumed int
	// claimed hands a peer-opened session to exactly one consumer (the edge
	// claims connector-initiated http-stream sessions this way).
	claimed atomic.Bool
}

func newSession(id string, link *Link) *Session {
	s := &Session{
		id:     id,
		link:   link,
		recv:   make(chan tunnel.Frame, sendWindow+recvSlack),
		done:   make(chan struct{}),
		credit: make(chan struct{}, sendWindow),
	}
	for i := 0; i < sendWindow; i++ {
		s.credit <- struct{}{}
	}
	return s
}

func (s *Session) ID() string { return s.id }

// TryClaim marks the session as owned by the caller; it succeeds exactly once.
func (s *Session) TryClaim() bool { return !s.claimed.Swap(true) }

// Claimed reports whether any consumer has claimed the session.
func (s *Session) Claimed() bool { return s.claimed.Load() }

// Recv delivers data and control frames for this session in arrival order.
func (s *Session) Recv() <-chan tunnel.Frame { return s.recv }

// Done is closed when the session is removed from its link.
func (s *Session) Done() <-chan struct{} { return s.done }

// SendData transmits one data frame, blocking while the send window is
// exhausted until the peer returns credit or the session closes.
func (s *Session) SendData(dataKind string, payload []byte) error {
	select {
	case <-s.credit:
	case <-s.done:
		return ErrSessionClosed
	}
	return s.link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameData, SessionID: s.id, DataKind: dataKind, Payload: payload})
}

// SendEOF signals a half-close: no more data follows in this direction.
func (s *Session) SendEOF() error {
	return s.link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameEOF, SessionID: s.id})
}

// AckData returns window credit to the peer. The consumer must call it after
// fully processing each data frame taken from Recv.
func (s *Session) AckData() {
	s.consumed++
	if s.consumed >= ackThreshold {
		_ = s.link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameWindow, SessionID: s.id, Window: s.consumed})
		s.consumed = 0
	}
}

func (s *Session) addSendCredit(n int) {
	for i := 0; i < n; i++ {
		select {
		case s.credit <- struct{}{}:
		default:
			// Excess credit from a buggy peer; cap at the window size.
			return
		}
	}
}

// Close notifies the peer and removes the session from the link. Safe to call
// multiple times and from any goroutine.
func (s *Session) Close() {
	_ = s.link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameClose, SessionID: s.id})
	s.link.removeSession(s.id)
}

// CloseWithError reports a failure to the peer and removes the session.
func (s *Session) CloseWithError(message string) {
	_ = s.link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: s.id, Error: message})
	s.link.removeSession(s.id)
}

// CloseWithCode closes the session carrying a WebSocket close status so the
// other end can finish its close handshake with the original code and text.
func (s *Session) CloseWithCode(code int, text string) {
	_ = s.link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameClose, SessionID: s.id, CloseCode: code, Payload: []byte(text)})
	s.link.removeSession(s.id)
}

func (s *Session) markDone() {
	s.once.Do(func() { close(s.done) })
}
