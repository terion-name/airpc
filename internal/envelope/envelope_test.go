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
		encoded, err := MarshalOpenRequest(OpenRequest{RequestID: "req-1", SessionID: "sess-1", Route: "demo", Headers: header})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := UnmarshalOpenRequest(encoded, 0)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.SessionID != "sess-1" || got.Route != "demo" {
			t.Fatalf("unexpected open request: %#v", got)
		}
	})
	t.Run("open response", func(t *testing.T) {
		encoded, err := MarshalOpenResponse(OpenResponse{RequestID: "req-1", SessionID: "sess-1", Accepted: true, Headers: header})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got, err := UnmarshalOpenResponse(encoded, 0)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !got.Accepted || got.SessionID != "sess-1" {
			t.Fatalf("unexpected open response: %#v", got)
		}
	})
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

func TestDecodeRejectsRouteRequestAndSessionValidation(t *testing.T) {
	if _, err := UnmarshalUnaryRequest(unaryRequestArray(t, 1, "bad id", "demo", "GET", "/", http.Header{}, nil), 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad request id error = %v", err)
	}
	if _, err := UnmarshalUnaryRequest(unaryRequestArray(t, 1, "req-1", "bad.route", "GET", "/", http.Header{}, nil), 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad route error = %v", err)
	}
	encoded, err := MarshalOpenRequest(OpenRequest{RequestID: "req-1", SessionID: "bad session", Route: "demo"})
	if err == nil {
		t.Fatalf("MarshalOpenRequest accepted bad session: %x", encoded)
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	must(t, enc.EncodeArrayLen(5))
	must(t, enc.EncodeUint8(1))
	must(t, enc.EncodeString("req-1"))
	must(t, enc.EncodeString("bad session"))
	must(t, enc.EncodeString("demo"))
	must(t, encodeHeader(enc, http.Header{}))
	if _, err := UnmarshalOpenRequest(buf.Bytes(), 0); !errors.Is(err, ErrInvalid) {
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
