package httpunary

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRequestRoundTripValidatesEnvelope(t *testing.T) {
	req := Request{
		Version:        Version,
		RequestID:      "req-1",
		Route:          "demo",
		DeadlineUnixMS: time.Now().Add(time.Second).UnixMilli(),
		Method:         http.MethodPost,
		Scheme:         "https",
		Authority:      "example.com",
		Path:           "/api?x=1",
		Headers:        http.Header{"Accept": {"application/json"}},
		Body:           []byte("hello"),
	}
	data, err := EncodeRequest(req)
	if err != nil {
		t.Fatalf("EncodeRequest(): %v", err)
	}
	got, err := DecodeRequest(data)
	if err != nil {
		t.Fatalf("DecodeRequest(): %v", err)
	}
	if got.RequestID != req.RequestID || got.Path != req.Path || string(got.Body) != "hello" {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestResponseRoundTripValidatesEnvelope(t *testing.T) {
	resp := Response{
		Version:   Version,
		RequestID: "req-1",
		Status:    http.StatusCreated,
		Headers:   http.Header{"Content-Type": {"text/plain"}},
		Body:      []byte("created"),
	}
	data, err := EncodeResponse(resp)
	if err != nil {
		t.Fatalf("EncodeResponse(): %v", err)
	}
	got, err := DecodeResponse(data)
	if err != nil {
		t.Fatalf("DecodeResponse(): %v", err)
	}
	if got.Status != http.StatusCreated || string(got.Body) != "created" {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestValidationRejectsInvalidEnvelopes(t *testing.T) {
	valid := Request{
		Version:        Version,
		RequestID:      "req-1",
		Route:          "demo",
		DeadlineUnixMS: time.Now().Add(time.Second).UnixMilli(),
		Method:         http.MethodGet,
		Scheme:         "http",
		Authority:      "example.com",
		Path:           "/",
	}
	tests := []struct {
		name string
		req  Request
		want string
	}{
		{name: "bad version", req: withRequest(valid, func(r *Request) { r.Version = 2 }), want: "version"},
		{name: "bad route", req: withRequest(valid, func(r *Request) { r.Route = "bad.route" }), want: "route"},
		{name: "bad path", req: withRequest(valid, func(r *Request) { r.Path = "relative" }), want: "path"},
		{name: "bad header", req: withRequest(valid, func(r *Request) { r.Headers = http.Header{"Bad Header": {"x"}} }), want: "header"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeRequest(tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("EncodeRequest() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestErrorResponseValidation(t *testing.T) {
	resp := NewErrorResponse("req-1", "backend_error", "backend failed")
	data, err := EncodeResponse(resp)
	if err != nil {
		t.Fatalf("EncodeResponse(error): %v", err)
	}
	got, err := DecodeResponse(data)
	if err != nil {
		t.Fatalf("DecodeResponse(error): %v", err)
	}
	if got.Error == nil || got.Error.Code != "backend_error" {
		t.Fatalf("error response = %#v", got)
	}
}

func withRequest(req Request, mutate func(*Request)) Request {
	mutate(&req)
	return req
}
