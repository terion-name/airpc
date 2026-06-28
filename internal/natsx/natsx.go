package natsx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	HeaderErrorKind    = "Airpc-Error"
	HeaderErrorMessage = "Airpc-Error-Message"

	KindProtocol = "protocol"
	KindRejected = "rejected"
)

var (
	ErrNoResponders = errors.New("nats no responders")
	ErrTimeout      = errors.New("nats timeout")
	ErrProtocol     = errors.New("airpc protocol error")
	ErrRejected     = errors.New("airpc request rejected")
	ErrNotConnected = errors.New("nats not connected")
)

type Handler func(context.Context, *nats.Msg) error

type Subscription struct {
	sub *nats.Subscription
}

func Connect(ctx context.Context, url string, opts ...nats.Option) (*nats.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	connectOpts := []nats.Option{
		nats.Name("airpc"),
		nats.RetryOnFailedConnect(false),
		nats.Timeout(timeoutFor(ctx)),
	}
	connectOpts = append(connectOpts, opts...)
	conn, err := nats.Connect(url, connectOpts...)
	if err != nil {
		return nil, MapError(err)
	}
	if err := ctx.Err(); err != nil {
		conn.Close()
		return nil, MapError(err)
	}
	return conn, nil
}

func Request(ctx context.Context, conn *nats.Conn, subject string, data []byte) (*nats.Msg, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if conn == nil || conn.IsClosed() {
		return nil, ErrNotConnected
	}
	msg, err := conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, MapError(err)
	}
	if err := ErrorFromHeaders(msg.Header); err != nil {
		return nil, err
	}
	return msg, nil
}

func Reply(msg *nats.Msg, data []byte) error {
	if msg == nil {
		return fmt.Errorf("reply message is nil")
	}
	if err := msg.Respond(data); err != nil {
		return MapError(err)
	}
	return nil
}

func ReplyError(msg *nats.Msg, kind error, message string) error {
	if msg == nil {
		return fmt.Errorf("reply message is nil")
	}
	errorKind := KindProtocol
	if errors.Is(kind, ErrRejected) {
		errorKind = KindRejected
	}
	reply := nats.NewMsg(msg.Reply)
	reply.Header.Set(HeaderErrorKind, errorKind)
	reply.Header.Set(HeaderErrorMessage, message)
	if err := msg.RespondMsg(reply); err != nil {
		return MapError(err)
	}
	return nil
}

func QueueSubscribe(ctx context.Context, conn *nats.Conn, subject, queue string, handle Handler) (*Subscription, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if conn == nil || conn.IsClosed() {
		return nil, ErrNotConnected
	}
	if subject == "" {
		return nil, fmt.Errorf("subject is required")
	}
	if queue == "" {
		return nil, fmt.Errorf("queue is required")
	}
	if handle == nil {
		return nil, fmt.Errorf("handler is required")
	}
	sub, err := conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		_ = handle(ctx, msg)
	})
	if err != nil {
		return nil, MapError(err)
	}
	if err := conn.FlushTimeout(timeoutFor(ctx)); err != nil {
		_ = sub.Unsubscribe()
		return nil, MapError(err)
	}
	return &Subscription{sub: sub}, nil
}

func (s *Subscription) Drain(ctx context.Context) error {
	if s == nil || s.sub == nil {
		return nil
	}
	return runWithContext(ctx, func() error { return s.sub.Drain() }, func() { _ = s.sub.Unsubscribe() })
}

func Drain(ctx context.Context, conn *nats.Conn) error {
	if conn == nil {
		return nil
	}
	return runWithContext(ctx, conn.Drain, conn.Close)
}

func MapError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return fmt.Errorf("%w: %v", ErrNoResponders, err)
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	default:
		return err
	}
}

func ErrorFromHeaders(h nats.Header) error {
	switch h.Get(HeaderErrorKind) {
	case "":
		return nil
	case KindProtocol:
		return fmt.Errorf("%w: %s", ErrProtocol, h.Get(HeaderErrorMessage))
	case KindRejected:
		return fmt.Errorf("%w: %s", ErrRejected, h.Get(HeaderErrorMessage))
	default:
		return fmt.Errorf("%w: unknown reply error kind %q", ErrProtocol, h.Get(HeaderErrorKind))
	}
}

func timeoutFor(ctx context.Context) time.Duration {
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining > 0 {
				return remaining
			}
			return time.Nanosecond
		}
	}
	return 2 * time.Second
}

func runWithContext(ctx context.Context, run func() error, cancel func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	go func() {
		done <- run()
	}()
	select {
	case err := <-done:
		return MapError(err)
	case <-ctx.Done():
		cancel()
		return MapError(ctx.Err())
	}
}
