package httpheaders

import (
	"net/http"
	"strings"
)

var hopByHop = map[string]struct{}{
	"connection":          {},
	"keep-alive":          {},
	"proxy-authenticate":  {},
	"proxy-authorization": {},
	"te":                  {},
	"trailer":             {},
	"transfer-encoding":   {},
	"upgrade":             {},
}

// FilterRequest copies only explicitly allowed end-to-end headers from an inbound request.
func FilterRequest(src http.Header, allowed []string) http.Header {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" || isHopByHop(canonical) {
			continue
		}
		allowedSet[canonical] = struct{}{}
	}

	dst := make(http.Header)
	for name, values := range src {
		canonical := http.CanonicalHeaderKey(name)
		if _, ok := allowedSet[canonical]; !ok || isHopByHop(canonical) {
			continue
		}
		copyValues(dst, canonical, values)
	}
	return dst
}

// FilterResponse strips hop-by-hop headers before the edge writes a backend response to a client.
func FilterResponse(src http.Header) http.Header {
	dst := make(http.Header)
	for name, values := range src {
		canonical := http.CanonicalHeaderKey(name)
		if canonical == "" || isHopByHop(canonical) {
			continue
		}
		copyValues(dst, canonical, values)
	}
	return dst
}

func copyValues(dst http.Header, name string, values []string) {
	for _, value := range values {
		dst.Add(name, value)
	}
}

func isHopByHop(name string) bool {
	_, ok := hopByHop[strings.ToLower(name)]
	return ok
}
