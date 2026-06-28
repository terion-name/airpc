package natsx

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestMapError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"no responders", nats.ErrNoResponders, ErrNoResponders},
		{"nats timeout", nats.ErrTimeout, ErrTimeout},
		{"context deadline", context.DeadlineExceeded, ErrTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(MapError(tc.err), tc.want) {
				t.Fatalf("MapError(%v) = %v, want %v", tc.err, MapError(tc.err), tc.want)
			}
		})
	}
}

func TestErrorFromHeaders(t *testing.T) {
	tests := []struct {
		name string
		kind string
		want error
	}{
		{"protocol", KindProtocol, ErrProtocol},
		{"rejected", KindRejected, ErrRejected},
		{"unknown", "weird", ErrProtocol},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := nats.Header{}
			h.Set(HeaderErrorKind, tc.kind)
			h.Set(HeaderErrorMessage, "nope")
			if !errors.Is(ErrorFromHeaders(h), tc.want) {
				t.Fatalf("ErrorFromHeaders() = %v, want %v", ErrorFromHeaders(h), tc.want)
			}
		})
	}
	if err := ErrorFromHeaders(nats.Header{}); err != nil {
		t.Fatalf("empty headers error = %v", err)
	}
}

func TestDrainNilIsNoop(t *testing.T) {
	if err := (*Subscription)(nil).Drain(context.Background()); err != nil {
		t.Fatalf("nil subscription drain: %v", err)
	}
	if err := Drain(context.Background(), nil); err != nil {
		t.Fatalf("nil conn drain: %v", err)
	}
}
