package envelope

import (
	"bytes"
	"errors"
	"net/http"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestEnvelopeRoundTrips(t *testing.T) {
	header := http.Header{"Content-Type": {"application/msgpack"}}
	t.Run("unary request", func(t *testing.T) {
		encoded, err := MarshalUnaryRequest(UnaryRequest{RequestID: "req-1", Route: "demo", Method: "POST", Path: "/rpc", Headers: header, Body: []byte("body")})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := UnmarshalUnaryRequest(encoded, 0)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.RequestID != "req-1" || got.Route != "demo" || string(got.Body) != "body" {
			t.Fatalf("unexpected request: %#v", got)
		}
	})
	t.Run("unary response", func(t *testing.T) {
		encoded, err := MarshalUnaryResponse(UnaryResponse{RequestID: "req-1", Status: 200, Headers: header, Body: []byte("ok")})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := UnmarshalUnaryResponse(encoded, 0)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.Status != 200 || string(got.Body) != "ok" {
			t.Fatalf("unexpected response: %#v", got)
		}
	})
	t.Run("open request", func(t *testing.T) {
		encoded, err := MarshalOpenRequest(OpenRequest{
			RequestID:      "req-1",
			SessionID:      "sess-1",
			Route:          "demo",
			Kind:           KindWebSocket,
			DeadlineUnixMS: 1_700_000_000_000,
			Host:           "example.com",
			Path:           "/ws",
			RawQuery:       "room=1",
			Headers:        header,
			TraceParent:    "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
			TraceState:     "rojo=00f067aa0ba902b7",
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := UnmarshalOpenRequest(encoded, 0)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.SessionID != "sess-1" || got.Route != "demo" || got.Kind != KindWebSocket || got.Path != "/ws" || got.RawQuery != "room=1" || got.TraceParent == "" {
			t.Fatalf("unexpected open request: %#v", got)
		}
	})
	t.Run("open response", func(t *testing.T) {
		encoded, err := MarshalOpenResponse(OpenResponse{RequestID: "req-1", SessionID: "sess-1", Route: "demo", ConnectorID: "local-1", Accepted: true, Headers: header})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := UnmarshalOpenResponse(encoded, 0)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !got.Accepted || got.SessionID != "sess-1" || got.ConnectorID != "local-1" || got.Route != "demo" {
			t.Fatalf("unexpected open response: %#v", got)
		}
	})
}

func TestTunnelFrameRoundTrip(t *testing.T) {
	frame := TunnelFrame{Type: FrameOpen, SessionID: "sess-1", Kind: KindTCP, Flags: 3, Opcode: OpcodeBinary, Payload: []byte("hello"), Code: 1000, Reason: "ok"}
	encoded, err := MarshalTunnelFrame(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalTunnelFrame(encoded, 0, 0)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != frame.Type || got.SessionID != frame.SessionID || got.Kind != frame.Kind || got.Flags != frame.Flags || got.Opcode != frame.Opcode || string(got.Payload) != "hello" || got.Code != frame.Code || got.Reason != frame.Reason {
		t.Fatalf("unexpected frame: %#v", got)
	}
}

func TestDecodeRejectsVersionLengthTrailingAndSize(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		max  int
		want error
	}{
		{name: "bad version", data: unaryRequestArray(t, 2, "req-1", "demo", "GET", "/", http.Header{}, nil), want: ErrInvalid},
		{name: "bad length", data: arrayWithOnlyVersion(t, 1, 6), want: ErrInvalid},
		{name: "trailing", data: append(unaryRequestArray(t, 1, "req-1", "demo", "GET", "/", http.Header{}, nil), 0xc0), want: ErrInvalid},
		{name: "too large", data: unaryRequestArray(t, 1, "req-1", "demo", "GET", "/", http.Header{}, []byte("abcdef")), max: 5, want: ErrTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalUnaryRequest(tc.data, tc.max)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestTunnelFrameRejectsInvalidData(t *testing.T) {
	valid := TunnelFrame{Type: FrameData, SessionID: "sess-1", Opcode: OpcodeBinary, Payload: []byte("abc")}
	encoded, err := MarshalTunnelFrame(valid)
	if err != nil {
		t.Fatalf("marshal valid frame: %v", err)
	}
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "bad version", data: tunnelFrameArray(t, 2, FrameData, "sess-1", "", 0, OpcodeBinary, nil, 0, ""), want: ErrInvalid},
		{name: "wrong length", data: arrayWithOnlyVersion(t, 1, 8), want: ErrInvalid},
		{name: "trailing", data: append(encoded, 0xc0), want: ErrInvalid},
		{name: "invalid session", data: tunnelFrameArray(t, 1, FrameData, "bad session", "", 0, OpcodeBinary, nil, 0, ""), want: ErrInvalid},
		{name: "invalid kind", data: tunnelFrameArray(t, 1, FrameOpen, "sess-1", "http", 0, OpcodeNone, nil, 0, ""), want: ErrInvalid},
		{name: "invalid opcode", data: tunnelFrameArray(t, 1, FrameData, "sess-1", "", 0, 99, nil, 0, ""), want: ErrInvalid},
		{name: "kind on data", data: tunnelFrameArray(t, 1, FrameData, "sess-1", KindTCP, 0, OpcodeBinary, nil, 0, ""), want: ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalTunnelFrame(tc.data, 0, 0)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
	if _, err := UnmarshalTunnelFrame(tunnelFrameArray(t, 1, FrameData, "sess-1", "", 0, OpcodeBinary, []byte("abcdef"), 0, ""), 1024, 5); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize payload error = %v", err)
	}
}

func TestDecodeRejectsRouteRequestAndSessionValidation(t *testing.T) {
	if _, err := UnmarshalUnaryRequest(unaryRequestArray(t, 1, "bad id", "demo", "GET", "/", http.Header{}, nil), 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad request id error = %v", err)
	}
	if _, err := UnmarshalUnaryRequest(unaryRequestArray(t, 1, "req-1", "bad*route", "GET", "/", http.Header{}, nil), 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad route error = %v", err)
	}
	encoded, err := MarshalOpenRequest(OpenRequest{RequestID: "req-1", SessionID: "bad session", Route: "demo", Kind: KindTCP, DeadlineUnixMS: 1})
	if err == nil {
		t.Fatalf("MarshalOpenRequest accepted bad session: %x", encoded)
	}
	if _, err := UnmarshalOpenRequest(openRequestArray(t, 1, "req-1", "bad session", "demo", KindTCP, 1, "", "", "", http.Header{}, "", ""), 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad session error = %v", err)
	}
}

func unaryRequestArray(t *testing.T, version uint8, requestID, route, method, path string, h http.Header, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	must(t, enc.EncodeArrayLen(7))
	must(t, enc.EncodeUint8(version))
	for _, s := range []string{requestID, route, method, path} {
		must(t, enc.EncodeString(s))
	}
	must(t, encodeHeader(enc, h))
	must(t, enc.EncodeBytes(body))
	return buf.Bytes()
}

func openRequestArray(t *testing.T, version uint8, requestID, sessionID, route, kind string, deadline int64, host, path, rawQuery string, h http.Header, traceParent, traceState string) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	must(t, enc.EncodeArrayLen(12))
	must(t, enc.EncodeUint8(version))
	for _, s := range []string{requestID, sessionID, route, kind} {
		must(t, enc.EncodeString(s))
	}
	must(t, enc.EncodeInt64(deadline))
	for _, s := range []string{host, path, rawQuery} {
		must(t, enc.EncodeString(s))
	}
	must(t, encodeHeader(enc, h))
	must(t, enc.EncodeString(traceParent))
	must(t, enc.EncodeString(traceState))
	return buf.Bytes()
}

func tunnelFrameArray(t *testing.T, version uint8, frameType, sessionID, kind string, flags uint32, opcode uint8, payload []byte, code uint16, reason string) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	must(t, enc.EncodeArrayLen(9))
	must(t, enc.EncodeUint8(version))
	must(t, enc.EncodeString(frameType))
	must(t, enc.EncodeString(sessionID))
	must(t, enc.EncodeString(kind))
	must(t, enc.EncodeUint64(uint64(flags)))
	must(t, enc.EncodeUint8(opcode))
	must(t, enc.EncodeBytes(payload))
	must(t, enc.EncodeUint16(code))
	must(t, enc.EncodeString(reason))
	return buf.Bytes()
}

func arrayWithOnlyVersion(t *testing.T, version uint8, length int) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	must(t, enc.EncodeArrayLen(length))
	must(t, enc.EncodeUint8(version))
	return buf.Bytes()
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
