package subject

import (
	"fmt"
	"strings"
	"unicode"
)

const routePrefix = "airpc.v1.route."

func Unary(route string) (string, error) {
	if err := ValidateRouteName(route); err != nil {
		return "", err
	}
	return routePrefix + route + ".unary", nil
}

func Open(route string) (string, error) {
	if err := ValidateRouteName(route); err != nil {
		return "", err
	}
	return routePrefix + route + ".open", nil
}

func ConnectorQueue(route string) (string, error) {
	if err := ValidateRouteName(route); err != nil {
		return "", err
	}
	return "airpc.route." + route + ".connectors", nil
}

func MustUnary(route string) string {
	s, err := Unary(route)
	if err != nil {
		panic(err)
	}
	return s
}

func MustOpen(route string) string {
	s, err := Open(route)
	if err != nil {
		panic(err)
	}
	return s
}

func MustConnectorQueue(route string) string {
	s, err := ConnectorQueue(route)
	if err != nil {
		panic(err)
	}
	return s
}

func ValidateRouteName(route string) error {
	if route == "" {
		return fmt.Errorf("route name is required")
	}
	if len(route) > 127 {
		return fmt.Errorf("route name %q exceeds 127 bytes", route)
	}
	if strings.ContainsAny(route, "*>") || strings.HasPrefix(route, ".") || strings.HasSuffix(route, ".") || strings.Contains(route, "..") {
		return fmt.Errorf("route name %q is not a safe NATS token path", route)
	}
	for _, r := range route {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return fmt.Errorf("route name %q must use letters, digits, '.', '-' or '_'", route)
		}
	}
	return nil
}

func ValidateSubject(s string) error {
	if s == "" {
		return fmt.Errorf("subject is required")
	}
	if strings.TrimSpace(s) != s {
		return fmt.Errorf("subject %q has surrounding whitespace", s)
	}
	if strings.ContainsAny(s, "*>") {
		return fmt.Errorf("subject %q must not contain wildcards", s)
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("subject %q contains whitespace or control character", s)
		}
	}
	for _, token := range strings.Split(s, ".") {
		if token == "" {
			return fmt.Errorf("subject %q contains an empty token", s)
		}
	}
	return nil
}
