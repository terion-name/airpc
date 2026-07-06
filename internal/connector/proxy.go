package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/httpheaders"
	"github.com/terion-name/airpc/internal/protocol/httpunary"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
)

const defaultResponseLimit = 16 << 20

type route struct {
	cfg    config.Route
	target *url.URL
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

// httpProxy forwards HTTP unary envelopes to private backends and tracks
// in-flight requests so edge-published cancels abort the backend call.
type httpProxy struct {
	connectorID string
	client      *http.Client
	routes      map[string]route
	tunnel      *tunnelClient // for streaming oversized responses; may be nil

	mu       sync.Mutex
	inflight map[string]context.CancelFunc
}

func newHTTPProxy(connectorID string, routes []route, tunnel *tunnelClient) *httpProxy {
	byName := make(map[string]route, len(routes))
	for _, r := range routes {
		byName[r.cfg.Name] = r
	}
	return &httpProxy{
		connectorID: connectorID,
		client:      &http.Client{},
		routes:      byName,
		tunnel:      tunnel,
		inflight:    make(map[string]context.CancelFunc),
	}
}

func (p *httpProxy) handle(parent context.Context, data []byte) httpunary.Response {
	req, err := httpunary.DecodeRequest(data)
	if err != nil {
		return httpunary.NewErrorResponse("unknown", "protocol_error", "invalid request envelope")
	}
	r, ok := p.routes[req.Route]
	if !ok {
		return httpunary.NewErrorResponse(req.RequestID, "unknown_route", "route is not configured on connector")
	}
	return p.forward(parent, r, req)
}

func (p *httpProxy) cancel(requestID string) {
	p.mu.Lock()
	cancel := p.inflight[requestID]
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// track registers a cancel func under the request ID and returns its
// deregistration func.
func (p *httpProxy) track(requestID string, cancel context.CancelFunc) func() {
	p.mu.Lock()
	p.inflight[requestID] = cancel
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		delete(p.inflight, requestID)
		p.mu.Unlock()
	}
}

func (p *httpProxy) forward(parent context.Context, r route, req httpunary.Request) httpunary.Response {
	backendCtx, cancelBackend := context.WithCancel(parent)
	untrack := p.track(req.RequestID, cancelBackend)
	streaming := false
	defer func() {
		if !streaming {
			cancelBackend()
			untrack()
		}
	}()
	// The request deadline bounds response headers and any inline body read;
	// a timer instead of context.WithDeadline lets a streamed body detach
	// from the deadline once headers have been sent back to the edge.
	deadlineTimer := time.AfterFunc(time.Until(time.UnixMilli(req.DeadlineUnixMS)), cancelBackend)
	defer deadlineTimer.Stop()

	backendURL, err := targetURL(r.target, req.Path)
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "protocol_error", "invalid request path")
	}
	backendReq, err := http.NewRequestWithContext(backendCtx, req.Method, backendURL.String(), bytes.NewReader(req.Body))
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "protocol_error", "failed to build backend request")
	}
	backendReq.Header = req.Headers.Clone()

	backendResp, err := p.client.Do(backendReq)
	if err != nil {
		return httpunary.NewErrorResponse(req.RequestID, "backend_error", "backend request failed")
	}

	if shouldStream(backendResp, routeResponseLimit(r.cfg)) {
		if resp, ok := p.streamResponse(req, backendResp, cancelBackend, untrack); ok {
			streaming = true
			return resp
		}
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

// shouldStream picks the tunnel for bodies that cannot or should not be
// inlined in the NATS reply: unknown length (chunked, SSE, EOF-delimited),
// declared length above the inline limit, or an event stream regardless of
// size.
func shouldStream(resp *http.Response, inlineLimit int64) bool {
	if resp.ContentLength < 0 || resp.ContentLength > inlineLimit {
		return true
	}
	return strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

// streamResponse opens an http-stream session on the data tunnel and pumps
// the backend body through it. On success the caller must not touch the
// response body again: the pump goroutine owns it, along with the request's
// cancel and inflight-tracking cleanup. Returns ok=false (nothing consumed,
// nothing cleaned up) when no tunnel is available, so the caller can fall
// back to the inline path.
func (p *httpProxy) streamResponse(req httpunary.Request, backendResp *http.Response, cancelBackend context.CancelFunc, untrack func()) (httpunary.Response, bool) {
	if p.tunnel == nil {
		return httpunary.Response{}, false
	}
	link := p.tunnel.currentLink()
	if link == nil {
		return httpunary.Response{}, false
	}
	frame := tunnel.Frame{Version: tunnel.Version, Type: tunnel.FrameOpen, SessionID: req.RequestID, Route: req.Route, Kind: tunnel.KindHTTPStream}
	session, err := link.Open(frame)
	if err != nil {
		return httpunary.Response{}, false
	}

	// A vanished reader (client disconnect, edge restart) must abort a
	// blocked body read, not just the next SendData.
	go func() {
		<-session.Done()
		cancelBackend()
	}()
	go func() {
		defer untrack()
		defer cancelBackend()
		defer backendResp.Body.Close()
		buf := make([]byte, 32*1024)
		for {
			n, err := backendResp.Body.Read(buf)
			if n > 0 {
				if session.SendData(tunnel.DataBinary, buf[:n]) != nil {
					return
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					_ = session.SendEOF()
					session.Close()
				} else {
					session.CloseWithError("backend body read failed")
				}
				return
			}
		}
	}()

	return httpunary.Response{
		Version:         httpunary.Version,
		RequestID:       req.RequestID,
		Status:          backendResp.StatusCode,
		Headers:         httpheaders.FilterResponse(backendResp.Header),
		StreamSessionID: req.RequestID,
		ConnectorID:     p.connectorID,
	}, true
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
