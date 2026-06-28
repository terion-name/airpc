package ids

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"strings"
)

const maxLen = 128

var noPaddingBase32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// New returns a random, URL- and subject-safe identifier.
func New() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return strings.ToLower(noPaddingBase32.EncodeToString(b[:])), nil
}

func Validate(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", kind)
	}
	if len(value) > maxLen {
		return fmt.Errorf("%s exceeds %d bytes", kind, maxLen)
	}
	for _, r := range value {
		if !validRune(r) {
			return fmt.Errorf("%s contains invalid character %q", kind, r)
		}
	}
	return nil
}

func validRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r >= '0' && r <= '9':
		return true
	case r == '-' || r == '_' || r == '.':
		return true
	default:
		return false
	}
}
