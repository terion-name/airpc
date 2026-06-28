package natscore

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

var (
	ErrNoResponders = nats.ErrNoResponders
	ErrTimeout      = nats.ErrTimeout
)

type Client struct {
	nc *nats.Conn
}

type Msg struct {
	Subject string
	Reply   string
	Data    []byte
	msg     *nats.Msg
}

type Subscription struct {
	sub *nats.Subscription
}

func Connect(url, name string) (*Client, error) {
	nc, err := nats.Connect(url, nats.Name(name))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	return &Client{nc: nc}, nil
}

func (c *Client) Request(ctx context.Context, subject string, data []byte) ([]byte, error) {
	msg, err := c.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Data: data})
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (c *Client) QueueSubscribe(subject, queue string, handler func(Msg)) (*Subscription, error) {
	sub, err := c.nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		handler(Msg{Subject: msg.Subject, Reply: msg.Reply, Data: msg.Data, msg: msg})
	})
	if err != nil {
		return nil, fmt.Errorf("queue subscribe %s: %w", subject, err)
	}
	if err := c.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flush subscription %s: %w", subject, err)
	}
	return &Subscription{sub: sub}, nil
}

func (m Msg) Respond(data []byte) error {
	if m.msg == nil {
		return errors.New("respond NATS message: missing message")
	}
	return m.msg.Respond(data)
}

func (s *Subscription) Unsubscribe() error {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.Unsubscribe()
}

func (c *Client) Drain() error {
	if c == nil || c.nc == nil {
		return nil
	}
	return c.nc.Drain()
}

func (c *Client) Close() {
	if c != nil && c.nc != nil {
		c.nc.Close()
	}
}
