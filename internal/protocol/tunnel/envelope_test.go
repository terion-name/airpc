package tunnel

import (
	"bytes"
	"testing"
	"time"
)

func TestFrameRoundTrip(t *testing.T) {
	frame := Frame{Version: Version, Type: FrameData, SessionID: "sess-1", DataKind: DataBinary, Payload: []byte{0, 1, 2, 255}}
	data, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("EncodeFrame(): %v", err)
	}
	got, err := DecodeFrame(data)
	if err != nil {
		t.Fatalf("DecodeFrame(): %v", err)
	}
	if got.Type != frame.Type || got.SessionID != frame.SessionID || got.DataKind != frame.DataKind || !bytes.Equal(got.Payload, frame.Payload) {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestOpenRequestRoundTrip(t *testing.T) {
	req := OpenRequest{Version: Version, RequestID: "req-1", SessionID: "sess-1", Route: "echo", Kind: KindTCP, Path: "/", DeadlineUnixMS: time.Now().Add(time.Second).UnixMilli()}
	data, err := EncodeOpenRequest(req)
	if err != nil {
		t.Fatalf("EncodeOpenRequest(): %v", err)
	}
	got, err := DecodeOpenRequest(data)
	if err != nil {
		t.Fatalf("DecodeOpenRequest(): %v", err)
	}
	if got.RequestID != req.RequestID || got.SessionID != req.SessionID || got.Route != req.Route || got.Kind != req.Kind {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestFrameValidationRejectsInvalidKind(t *testing.T) {
	frame := Frame{Version: Version, Type: FrameOpen, SessionID: "sess-1", Route: "demo", Kind: "udp"}
	if _, err := EncodeFrame(frame); err == nil {
		t.Fatalf("EncodeFrame() err = nil, want invalid kind")
	}
}
