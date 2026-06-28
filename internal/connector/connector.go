package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/httpheaders"
	"github.com/terion-name/airpc/internal/natscore"
	"github.com/terion-name/airpc/internal/protocol/httpunary"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

const defaultResponseLimit = 16 << 20

type Connector struct {
	nats          *natscore.Client
	subscriptions []*natscore.Subscription
	dataTunnel    *connectorTunnel
	done          chan error
}

type route struct {
	cfg    config.Route
	target *url.URL
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
	nc, err := natscore.Connect(cfg.NATS.URL, "airpc-connector-"+id)
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
	byName := make(map[string]route, len(routes))
	for _, r := range routes {
		byName[r.cfg.Name] = r
	}
	client := &http.Client{}
	for _, r := range routes {
		routeCfg := r.cfg
		sub, err := nc.QueueSubscribe(routeCfg.UnarySubject(), routeCfg.QueueGroup(), func(msg natscore.Msg) {
			response := handleMessage(ctx, client, byName, msg.Data)
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
	for _, r := range streamByName {
		routeCfg := r.cfg
		sub, err := nc.QueueSubscribe(routeCfg.OpenSubject(), routeCfg.QueueGroup(), func(msg natscore.Msg) {
			payload := handleOpenMessage(id, streamByName, msg.Data)
			_ = msg.Respond(payload)
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		connector.subscriptions = append(connector.subscriptions, sub)
	}
	dataTunnel, err := dialConnectorTunnel(ctx, cfg, id, streamByName)
	if err != nil {
		cleanup()
		return nil, err
	}
	connector.dataTunnel = dataTunnel

	go func() {
		<-ctx.Done()
		for _, sub := range connector.subscriptions {
			_ = sub.Unsubscribe()
		}
		if connector.dataTunnel != nil {
			_ = connector.dataTunnel.conn.Close()
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
		fmt.Fprintf(started, "connector %s subscribed to %d routes (data tunnel: %t)\n", id, len(connector.subscriptions), connector.dataTunnel != nil)
	}
	return connector.Wait()
}

func (c *Connector) Wait() error {
	if c == nil || c.done == nil {
		return nil
	}
	return <-c.done
}

func handleOpenMessage(connectorID string, routes map[string]streamRoute, data []byte) []byte {
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
	payload, _ := tunnel.EncodeOpenResponse(tunnel.OpenResponse{Version: tunnel.Version, RequestID: req.RequestID, Accepted: true, ConnectorID: connectorID})
	return payload
}

func httpRoutes(routes []config.Route) ([]route, error) {
	out := make([]route, 0, len(routes))
	for _, r := range routes {
		if r.Mode != config.ModeHTTP {
			continue
		}
		target, err := url.Parse(r.Target)
		if err != nil {
			return nil, fmt.Errorf("parse route %s target: %w", r.Name, err)
		}
		out = append(out, route{cfg: r, target: target})
	}
	return out, nil
}

func handleMessage(parent context.Context, client *http.Client, routes map[string]route, data []byte) httpunary.Response {
	req, err := httpunary.DecodeRequest(data)
	if err != nil {
		return httpunary.NewErrorResponse("unknown", "protocol_error", "invalid request envelope")
	}
	r, ok := routes[req.Route]
	if !ok {
		return httpunary.NewErrorResponse(req.RequestID, "unknown_route", "route is not configured on connector")
	}
	return forward(parent, client, r, req)
}

func forward(parent context.Context, client *http.Client, r route, req httpunary.Request) httpunary.Response {
	deadline := time.UnixMilli(req.DeadlineUnixMS)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	backendURL, err := targetURL(r.target, req.Path)
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "protocol_error", "invalid request path")
	}
	backendReq, err := http.NewRequestWithContext(ctx, req.Method, backendURL.String(), bytes.NewReader(req.Body))
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "protocol_error", "failed to build backend request")
	}
	backendReq.Header = req.Headers.Clone()

	backendResp, err := client.Do(backendReq)
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "backend_error", "backend request failed")
	}
	defer backendResp.Body.Close()

	body, tooLarge, err := readBounded(backendResp.Body, routeResponseLimit(r.cfg))
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "backend_error", "failed to read backend response")
	}
	if tooLarge {
		return httpunary.NewErrorResponse(req.RequestID, "response_too_large", "backend response exceeds max_inline_response")
	}
	return httpunary.Response{
		Version:   httpunary.Version,
		RequestID: req.RequestID,
		Status:    backendResp.StatusCode,
		Headers:   httpheaders.FilterResponse(backendResp.Header),
		Body:      body,
	}
}

func targetURL(base *url.URL, suffix string) (*url.URL, error) {
	suffixURL, err := url.ParseRequestURI(suffix)
	if err != nil {
		return nil, err
	}
	out := *base
	basePath := strings.TrimRight(out.Path, "/")
	suffixPath := suffixURL.Path
	if suffixPath == "" {
		suffixPath = "/"
	}
	if basePath == "" {
		out.Path = suffixPath
	} else if suffixPath == "/" {
		out.Path = basePath + "/"
	} else {
		out.Path = basePath + suffixPath
	}
	out.RawPath = ""
	out.RawQuery = suffixURL.RawQuery
	out.Fragment = ""
	return &out, nil
}

func readBounded(body io.Reader, limit int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > limit {
		return nil, true, nil
	}
	return data, false, nil
}

func routeResponseLimit(r config.Route) int64 {
	limit := int64(r.MaxInlineResponse())
	if limit <= 0 {
		return defaultResponseLimit
	}
	return limit
}
