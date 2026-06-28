package headerx

import (
	"net/http"
	"reflect"
	"testing"
)

func TestFilterStripsHopByHopConnectionNominatedAndSensitive(t *testing.T) {
	in := http.Header{
		"Connection":          {"X-Hop, X-Also-Hop"},
		"Keep-Alive":          {"timeout=5"},
		"X-Hop":               {"drop"},
		"X-Also-Hop":          {"drop"},
		"Authorization":       {"secret"},
		"Cookie":              {"secret"},
		"Content-Type":        {"application/msgpack"},
		"X-Request-Id":        {"abc"},
		"X-Not-In-Allowlist":  {"drop"},
		"Proxy-Authorization": {"secret"},
	}
	got := Filter(in, []string{"content-type", "X-REQUEST-ID", "authorization", "keep-alive"})
	want := http.Header{
		"Content-Type": {"application/msgpack"},
		"X-Request-Id": {"abc"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Filter() = %#v, want %#v", got, want)
	}
}

func TestNormalizeAllowlist(t *testing.T) {
	got, err := NormalizeAllowlist([]string{"content-type", "X-Request-ID", "Authorization"})
	if err != nil {
		t.Fatalf("NormalizeAllowlist: %v", err)
	}
	if _, ok := got["Content-Type"]; !ok {
		t.Fatalf("Content-Type missing from %#v", got)
	}
	if _, ok := got["X-Request-Id"]; !ok {
		t.Fatalf("X-Request-Id missing from %#v", got)
	}
	if _, ok := got["Authorization"]; ok {
		t.Fatalf("Authorization should not be allowlisted")
	}
}
