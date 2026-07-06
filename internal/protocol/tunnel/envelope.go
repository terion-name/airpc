package tunnel

import (
	"fmt"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const Version = 1

const (
	FrameOpen   = "open"
	FrameData   = "data"
	FrameEOF    = "eof"
	FrameWindow = "window"
	FrameClose  = "close"
	FrameError  = "error"
)

const (
	KindTCP       = "tcp"
	KindWebSocket = "websocket"
	KindGRPC      = "grpc"
	// KindHTTPStream is a connector-initiated session carrying one streamed
	// HTTP unary response body (SSE, chunked, or oversized responses).
	KindHTTPStream = "http-stream"
)

const (
	DataBinary = "binary"
	DataText   = "text"
)

// ErrTunnelNotConnected is the open-rejection reason a connector reports
// while its data tunnel is down. The edge treats it as retryable, since
// another connector in the queue group may have a live tunnel.
const ErrTunnelNotConnected = "connector data tunnel is not connected"

// Frame is one binary-safe MessagePack message carried by the connector-owned
// data WebSocket. A single tunnel multiplexes many sessions by session_id.
//
// Flow control: each side may have at most a fixed number of unacknowledged
// data frames in flight per session. The receiver returns credit with
// FrameWindow after consuming data; Window is the number of frames credited.
// FrameEOF signals a half-close: no more data will follow in that direction,
// but the opposite direction stays open.
type Frame struct {
	Version   int    `msgpack:"version"`
	Type      string `msgpack:"type"`
	SessionID string `msgpack:"session_id"`
	Route     string `msgpack:"route,omitempty"`
	Kind      string `msgpack:"kind,omitempty"`
	DataKind  string `msgpack:"data_kind,omitempty"`
	Payload   []byte `msgpack:"payload,omitempty"`
	Window    int    `msgpack:"window,omitempty"`
	CloseCode int    `msgpack:"close_code,omitempty"`
	Error     string `msgpack:"error,omitempty"`
}

// OpenRequest is sent over NATS to choose a connector before the edge sends the
// matching FrameOpen over that connector's data tunnel.
type OpenRequest struct {
	Version        int    `msgpack:"version"`
	RequestID      string `msgpack:"request_id"`
	SessionID      string `msgpack:"session_id"`
	Route          string `msgpack:"route"`
	Kind           string `msgpack:"kind"`
	Path           string `msgpack:"path,omitempty"`
	DeadlineUnixMS int64  `msgpack:"deadline_unix_ms"`
}

type OpenResponse struct {
	Version     int    `msgpack:"version"`
	RequestID   string `msgpack:"request_id"`
	Accepted    bool   `msgpack:"accepted"`
	ConnectorID string `msgpack:"connector_id,omitempty"`
	Error       string `msgpack:"error,omitempty"`
}

func EncodeFrame(frame Frame) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, err
	}
	return msgpack.Marshal(frame)
}

func DecodeFrame(data []byte) (Frame, error) {
	var frame Frame
	if err := msgpack.Unmarshal(data, &frame); err != nil {
		return Frame{}, fmt.Errorf("decode tunnel frame: %w", err)
	}
	if err := frame.Validate(); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func EncodeOpenRequest(req OpenRequest) ([]byte, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return msgpack.Marshal(req)
}

func DecodeOpenRequest(data []byte) (OpenRequest, error) {
	var req OpenRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return OpenRequest{}, fmt.Errorf("decode tunnel open request: %w", err)
	}
	if err := req.Validate(); err != nil {
		return OpenRequest{}, err
	}
	return req, nil
}

func EncodeOpenResponse(resp OpenResponse) ([]byte, error) {
	if err := resp.Validate(); err != nil {
		return nil, err
	}
	return msgpack.Marshal(resp)
}

func DecodeOpenResponse(data []byte) (OpenResponse, error) {
	var resp OpenResponse
	if err := msgpack.Unmarshal(data, &resp); err != nil {
		return OpenResponse{}, fmt.Errorf("decode tunnel open response: %w", err)
	}
	if err := resp.Validate(); err != nil {
		return OpenResponse{}, err
	}
	return resp, nil
}

func NewFrame(frameType, sessionID string) Frame {
	return Frame{Version: Version, Type: frameType, SessionID: sessionID}
}

func (f Frame) Validate() error {
	if f.Version != Version {
		return fmt.Errorf("tunnel frame version %d is not supported", f.Version)
	}
	if !validToken(f.SessionID) {
		return fmt.Errorf("tunnel frame session_id %q is invalid", f.SessionID)
	}
	switch f.Type {
	case FrameOpen:
		if err := validateRouteAndKind(f.Route, f.Kind); err != nil {
			return err
		}
	case FrameData:
		if f.DataKind != "" && f.DataKind != DataBinary && f.DataKind != DataText {
			return fmt.Errorf("tunnel frame data_kind %q is invalid", f.DataKind)
		}
	case FrameEOF:
	case FrameWindow:
		if f.Window <= 0 {
			return fmt.Errorf("tunnel frame window %d is invalid", f.Window)
		}
	case FrameClose:
		// CloseCode carries a WebSocket close status; Payload carries the
		// close text. Zero means no code (TCP sessions, plain teardown).
		if f.CloseCode != 0 && (f.CloseCode < 1000 || f.CloseCode > 4999) {
			return fmt.Errorf("tunnel frame close_code %d is invalid", f.CloseCode)
		}
	case FrameError:
		if f.Error == "" || strings.ContainsAny(f.Error, "\r\n") {
			return fmt.Errorf("tunnel frame error is invalid")
		}
	default:
		return fmt.Errorf("tunnel frame type %q is invalid", f.Type)
	}
	return nil
}

func (r OpenRequest) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("tunnel open request version %d is not supported", r.Version)
	}
	if !validToken(r.RequestID) {
		return fmt.Errorf("tunnel open request request_id %q is invalid", r.RequestID)
	}
	if !validToken(r.SessionID) {
		return fmt.Errorf("tunnel open request session_id %q is invalid", r.SessionID)
	}
	if err := validateRouteAndKind(r.Route, r.Kind); err != nil {
		return err
	}
	if r.Path != "" && (!strings.HasPrefix(r.Path, "/") || strings.ContainsAny(r.Path, "\r\n#")) {
		return fmt.Errorf("tunnel open request path is invalid")
	}
	if r.DeadlineUnixMS <= 0 || time.UnixMilli(r.DeadlineUnixMS).Before(time.Unix(0, 0)) {
		return fmt.Errorf("tunnel open request deadline_unix_ms is invalid")
	}
	return nil
}

func (r OpenResponse) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("tunnel open response version %d is not supported", r.Version)
	}
	if !validToken(r.RequestID) {
		return fmt.Errorf("tunnel open response request_id %q is invalid", r.RequestID)
	}
	if r.Accepted {
		if !validToken(r.ConnectorID) {
			return fmt.Errorf("tunnel open response connector_id %q is invalid", r.ConnectorID)
		}
		return nil
	}
	if r.Error == "" || strings.ContainsAny(r.Error, "\r\n") {
		return fmt.Errorf("tunnel open response error is invalid")
	}
	return nil
}

func validateRouteAndKind(route, kind string) error {
	if !validToken(route) {
		return fmt.Errorf("tunnel route %q is invalid", route)
	}
	switch kind {
	case KindTCP, KindWebSocket, KindGRPC, KindHTTPStream:
		return nil
	default:
		return fmt.Errorf("tunnel kind %q is invalid", kind)
	}
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}
