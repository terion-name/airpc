package connector

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/datatunnel"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
	"github.com/terion-name/airpc/internal/telemetry"
)

const (
	tunnelDialTimeout = 10 * time.Second
	// reconnectBackoffMax caps the exponential redial backoff; a connection
	// that stayed up longer than reconnectStableAfter resets the backoff.
	reconnectBackoffMin  = 250 * time.Millisecond
	reconnectBackoffMax  = 5 * time.Second
	reconnectStableAfter = 30 * time.Second
)

type streamRoute struct {
	cfg    config.Route
	target *url.URL
}

func streamRoutes(routes []config.Route) (map[string]streamRoute, error) {
	out := make(map[string]streamRoute)
	for _, r := range routes {
		if r.Mode == config.ModeHTTP {
			continue
		}
		var target *url.URL
		if r.Mode == config.ModeWebSocket {
			u, err := url.Parse(r.Target)
			if err != nil {
				return nil, fmt.Errorf("parse route %s target: %w", r.Name, err)
			}
			target = u
		}
		out[r.Name] = streamRoute{cfg: r, target: target}
	}
	return out, nil
}

// tunnelClient owns the connector's outbound data WebSocket and keeps it
// alive: it redials with exponential backoff whenever the link drops, for the
// life of the context. While disconnected, open requests are rejected over
// NATS so the edge fails fast instead of finding a dead tunnel.
type tunnelClient struct {
	id      string
	dialURL string
	token   string
	dialer  *websocket.Dialer
	routes  map[string]streamRoute

	mu   sync.Mutex
	link *datatunnel.Link // nil while disconnected
}

// startTunnelClient dials the edge data tunnel; even HTTP-only connectors
// need it to stream large or SSE responses. It waits for the first dial
// attempt to finish (either way) so that a healthy setup is connected on
// return; network failures are then retried in the background rather than
// failing connector startup.
func startTunnelClient(ctx context.Context, cfg config.Config, id string, routes map[string]streamRoute) (*tunnelClient, error) {
	u, err := url.Parse(cfg.Connector.EdgeDataURL)
	if err != nil {
		return nil, fmt.Errorf("parse edge data url: %w", err)
	}
	query := u.Query()
	query.Set("connector_id", id)
	u.RawQuery = query.Encode()

	dialer := *websocket.DefaultDialer
	if cfg.Connector.TLS != nil {
		tlsCfg, err := cfg.Connector.TLS.ClientTLS()
		if err != nil {
			return nil, err
		}
		dialer.TLSClientConfig = tlsCfg
	}
	c := &tunnelClient{id: id, dialURL: u.String(), token: cfg.Connector.TunnelToken, dialer: &dialer, routes: routes}
	firstAttempt := make(chan struct{})
	go c.run(ctx, firstAttempt)
	select {
	case <-firstAttempt:
	case <-ctx.Done():
	}
	return c, nil
}

func (c *tunnelClient) run(ctx context.Context, firstAttempt chan<- struct{}) {
	attemptDone := func() {
		if firstAttempt != nil {
			close(firstAttempt)
			firstAttempt = nil
		}
	}
	var backoff time.Duration // zero: dial immediately
	for {
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		connectedAt := time.Now()
		link, err := c.dial(ctx)
		if err != nil {
			attemptDone()
			if ctx.Err() != nil {
				return
			}
			telemetry.TunnelDials.WithLabelValues(c.id, "failed").Inc()
			log.Printf("airpc connector %s: data tunnel dial failed: %v", c.id, err)
			backoff = nextBackoff(backoff)
			continue
		}
		c.setLink(link)
		telemetry.TunnelDials.WithLabelValues(c.id, "connected").Inc()
		telemetry.TunnelConnected.WithLabelValues(c.id).Set(1)
		log.Printf("airpc connector %s: data tunnel connected", c.id)
		attemptDone()

		runDone := make(chan struct{})
		go func() {
			link.Run()
			close(runDone)
		}()
		select {
		case <-ctx.Done():
			link.Close()
			<-runDone
			c.setLink(nil)
			telemetry.TunnelConnected.WithLabelValues(c.id).Set(0)
			return
		case <-runDone:
		}
		c.setLink(nil)
		telemetry.TunnelConnected.WithLabelValues(c.id).Set(0)
		log.Printf("airpc connector %s: data tunnel disconnected", c.id)
		if time.Since(connectedAt) > reconnectStableAfter {
			backoff = 0
		} else {
			backoff = nextBackoff(backoff)
		}
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current == 0 {
		return reconnectBackoffMin
	}
	next := current * 2
	if next > reconnectBackoffMax {
		next = reconnectBackoffMax
	}
	return next
}

func (c *tunnelClient) dial(ctx context.Context) (*datatunnel.Link, error) {
	requestHeader := http.Header{}
	if c.token != "" {
		requestHeader.Set("Authorization", "Bearer "+c.token)
	}
	dialCtx, cancel := context.WithTimeout(ctx, tunnelDialTimeout)
	defer cancel()
	conn, _, err := c.dialer.DialContext(dialCtx, c.dialURL, requestHeader)
	if err != nil {
		return nil, err
	}
	var link *datatunnel.Link
	link = datatunnel.NewLink(conn, func(frame tunnel.Frame) { c.handleOpen(ctx, link, frame) })
	return link, nil
}

func (c *tunnelClient) setLink(link *datatunnel.Link) {
	c.mu.Lock()
	c.link = link
	c.mu.Unlock()
}

func (c *tunnelClient) connected() bool {
	return c.currentLink() != nil
}

func (c *tunnelClient) currentLink() *datatunnel.Link {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.link
}

func (c *tunnelClient) handleOpen(ctx context.Context, link *datatunnel.Link, frame tunnel.Frame) {
	route, ok := c.routes[frame.Route]
	if !ok || route.cfg.Mode != frame.Kind {
		_ = link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: frame.SessionID, Error: "route is not configured"})
		return
	}
	session, err := link.Accept(frame.SessionID)
	if err != nil {
		_ = link.WriteFrame(tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameError, SessionID: frame.SessionID, Error: "session already exists"})
		return
	}
	go func() {
		switch route.cfg.Mode {
		case config.ModeTCP, config.ModeGRPC:
			relayTCPBackend(ctx, route, session)
		case config.ModeWebSocket:
			relayWebSocketBackend(ctx, route, session, string(frame.Payload))
		}
	}()
}

func relayTCPBackend(ctx context.Context, route streamRoute, session *datatunnel.Session) {
	backend, err := (&net.Dialer{}).DialContext(ctx, "tcp", route.cfg.Target)
	if err != nil {
		session.CloseWithError("backend dial failed")
		return
	}
	datatunnel.RelayConn(ctx, session, backend, route.cfg.IdleTimeout.Duration)
}

func relayWebSocketBackend(ctx context.Context, route streamRoute, session *datatunnel.Session, path string) {
	if path == "" {
		path = "/"
	}
	target, err := targetURL(route.target, path)
	if err != nil {
		session.CloseWithError("invalid websocket path")
		return
	}
	backend, _, err := websocket.DefaultDialer.DialContext(ctx, target.String(), http.Header{})
	if err != nil {
		session.CloseWithError("backend websocket dial failed")
		return
	}
	datatunnel.RelayWebSocket(ctx, session, backend, route.cfg.IdleTimeout.Duration)
}
