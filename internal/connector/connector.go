package connector

import (
	"context"
	"fmt"
	"io"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/natscore"
	"github.com/terion-name/airpc/internal/protocol/httpunary"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
	"github.com/terion-name/airpc/internal/telemetry"
)

type Connector struct {
	nats          *natscore.Client
	subscriptions []*natscore.Subscription
	tunnel        *tunnelClient
	metricsAddr   string
	done          chan error
}

func (c *Connector) MetricsAddr() string {
	if c == nil {
		return ""
	}
	return c.metricsAddr
}

func Start(ctx context.Context, cfg config.Config, id string) (*Connector, error) {
	routes, err := httpRoutes(cfg.Routes)
	if err != nil {
		return nil, err
	}
	streamByName, err := streamRoutes(cfg.Routes)
	if err != nil {
		return nil, err
	}
	nc, err := natscore.Connect(cfg.NATS.ConnectorURLOrDefault(), "airpc-connector-"+id, cfg.NATS.CredsFile)
	if err != nil {
		return nil, err
	}

	connector := &Connector{nats: nc, done: make(chan error, 1)}
	cleanup := func() {
		for _, sub := range connector.subscriptions {
			_ = sub.Unsubscribe()
		}
		nc.Close()
	}
	connector.metricsAddr, err = telemetry.StartServer(ctx, cfg.Connector.MetricsAddr)
	if err != nil {
		cleanup()
		return nil, err
	}

	dataTunnel, err := startTunnelClient(ctx, cfg, id, streamByName)
	if err != nil {
		cleanup()
		return nil, err
	}
	connector.tunnel = dataTunnel

	proxy := newHTTPProxy(id, routes, dataTunnel)
	for _, r := range routes {
		routeCfg := r.cfg
		sub, err := nc.QueueSubscribe(routeCfg.UnarySubject(), routeCfg.QueueGroup(), func(msg natscore.Msg) {
			response := proxy.handle(ctx, msg.Data)
			payload, err := httpunary.EncodeResponse(response)
			if err != nil {
				fallback := httpunary.NewErrorResponse(response.RequestID, "protocol_error", "failed to encode connector response")
				payload, _ = httpunary.EncodeResponse(fallback)
			}
			_ = msg.Respond(payload)
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		connector.subscriptions = append(connector.subscriptions, sub)
	}
	if len(routes) > 0 {
		sub, err := nc.Subscribe(config.CancelSubjectWildcard, func(msg natscore.Msg) {
			if requestID, ok := config.RequestIDFromCancelSubject(msg.Subject); ok {
				proxy.cancel(requestID)
			}
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		connector.subscriptions = append(connector.subscriptions, sub)
	}
	for _, r := range streamByName {
		routeCfg := r.cfg
		sub, err := nc.QueueSubscribe(routeCfg.OpenSubject(), routeCfg.QueueGroup(), func(msg natscore.Msg) {
			payload := handleOpenMessage(id, streamByName, dataTunnel, msg.Data)
			_ = msg.Respond(payload)
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		connector.subscriptions = append(connector.subscriptions, sub)
	}

	go func() {
		<-ctx.Done()
		for _, sub := range connector.subscriptions {
			_ = sub.Unsubscribe()
		}
		_ = nc.Drain()
		connector.done <- nil
		close(connector.done)
	}()
	return connector, nil
}

func Run(ctx context.Context, cfg config.Config, id string, started io.Writer) error {
	connector, err := Start(ctx, cfg, id)
	if err != nil {
		return err
	}
	if started != nil {
		fmt.Fprintf(started, "connector %s subscribed to %d routes (data tunnel: %t)\n", id, len(connector.subscriptions), connector.tunnel != nil && connector.tunnel.connected())
	}
	return connector.Wait()
}

func (c *Connector) Wait() error {
	if c == nil || c.done == nil {
		return nil
	}
	return <-c.done
}

func handleOpenMessage(connectorID string, routes map[string]streamRoute, tc *tunnelClient, data []byte) []byte {
	req, err := tunnel.DecodeOpenRequest(data)
	if err != nil {
		payload, _ := tunnel.EncodeOpenResponse(tunnel.OpenResponse{Version: tunnel.Version, RequestID: "unknown", Accepted: false, Error: "invalid open request"})
		return payload
	}
	r, ok := routes[req.Route]
	if !ok || r.cfg.Mode != req.Kind {
		payload, _ := tunnel.EncodeOpenResponse(tunnel.OpenResponse{Version: tunnel.Version, RequestID: req.RequestID, Accepted: false, Error: "route is not configured on connector"})
		return payload
	}
	if tc == nil || !tc.connected() {
		payload, _ := tunnel.EncodeOpenResponse(tunnel.OpenResponse{Version: tunnel.Version, RequestID: req.RequestID, Accepted: false, Error: tunnel.ErrTunnelNotConnected})
		return payload
	}
	payload, _ := tunnel.EncodeOpenResponse(tunnel.OpenResponse{Version: tunnel.Version, RequestID: req.RequestID, Accepted: true, ConnectorID: connectorID})
	return payload
}
