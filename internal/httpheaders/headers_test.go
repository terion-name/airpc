package httpheaders

import (
	"net/http"
	"testing"
)

func TestFilterRequestAllowsOnlyConfiguredEndToEndHeaders(t *testing.T) {
	src := http.Header{
		"Accept":          {"application/json"},
		"Authorization":   {"secret"},
		"Connection":      {"keep-alive"},
		"X-Request-Id":    {"req-1"},
		"X-Not-Forwarded": {"nope"},
	}
	got := FilterRequest(src, []string{"accept", "authorization", "connection", "x-request-id"})
	if got.Get("Accept") != "application/json" {
		t.Fatalf("Accept not forwarded: %#v", got)
	}
	if got.Get("X-Request-Id") != "req-1" {
		t.Fatalf("X-Request-Id not forwarded: %#v", got)
	}
	if got.Get("Connection") != "" || got.Get("X-Not-Forwarded") != "" {
		t.Fatalf("unexpected headers forwarded: %#v", got)
	}
	if got.Get("Authorization") != "secret" {
		t.Fatalf("configured Authorization should be forwarded without being logged: %#v", got)
	}
}

func TestFilterResponseStripsHopByHopHeaders(t *testing.T) {
	src := http.Header{
		"Content-Type":      {"text/plain"},
		"Transfer-Encoding": {"chunked"},
		"Upgrade":           {"websocket"},
	}
	got := FilterResponse(src)
	if got.Get("Content-Type") != "text/plain" {
		t.Fatalf("Content-Type not preserved: %#v", got)
	}
	if got.Get("Transfer-Encoding") != "" || got.Get("Upgrade") != "" {
		t.Fatalf("hop-by-hop headers not stripped: %#v", got)
	}
}
