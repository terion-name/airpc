package headerx

import (
	"fmt"
	"net/http"
	"net/textproto"
	"strings"
	"unicode"
)

var hopByHop = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

var sensitive = map[string]struct{}{
	"Authorization":       {},
	"Cookie":              {},
	"Proxy-Authorization": {},
	"Set-Cookie":          {},
	"X-Airpc-Token":       {},
}

func Filter(in http.Header, allowlist []string) http.Header {
	allow, _ := NormalizeAllowlist(allowlist)
	connectionTokens := nominatedByConnection(in)
	out := make(http.Header)
	for key, values := range in {
		canonical := textproto.CanonicalMIMEHeaderKey(key)
		if _, ok := hopByHop[canonical]; ok {
			continue
		}
		if _, ok := sensitive[canonical]; ok {
			continue
		}
		if _, ok := connectionTokens[canonical]; ok {
			continue
		}
		if len(allow) > 0 {
			if _, ok := allow[canonical]; !ok {
				continue
			}
		}
		for _, value := range values {
			out.Add(canonical, value)
		}
	}
	return out
}

func NormalizeAllowlist(names []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		canonical, err := NormalizeName(name)
		if err != nil {
			return nil, err
		}
		if _, ok := hopByHop[canonical]; ok {
			continue
		}
		if _, ok := sensitive[canonical]; ok {
			continue
		}
		out[canonical] = struct{}{}
	}
	return out, nil
}

func NormalizeName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("header name is required")
	}
	for _, r := range trimmed {
		if r > unicode.MaxASCII || !validTokenRune(byte(r)) {
			return "", fmt.Errorf("invalid header name %q", name)
		}
	}
	return textproto.CanonicalMIMEHeaderKey(trimmed), nil
}

func nominatedByConnection(h http.Header) map[string]struct{} {
	out := make(map[string]struct{})
	for _, value := range h.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			name := strings.TrimSpace(token)
			if name == "" {
				continue
			}
			canonical, err := NormalizeName(name)
			if err != nil {
				continue
			}
			out[canonical] = struct{}{}
		}
	}
	return out
}

func validTokenRune(r byte) bool {
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
		return true
	}
	switch r {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}
