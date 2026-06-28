package edge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/httpheaders"
	"github.com/terion-name/airpc/internal/natscore"
	"github.com/terion-name/airpc/internal/protocol/httpunary"
)

const (
	defaultRequestLimit = 1 << 20
	defaultTimeout      = 30 * time.Second
)

type Server struct {
	httpServer *http.Server
	listener   net.Listener
	nats       *natscore.Client
	done       chan error
}

type route struct {
	cfg    config.Route
	prefix string
	exact  string
}

func Start(ctx context.Context, cfg config.Config) (*Server, error) {
	nc, err := natscore.Connect(cfg.NATS.URL, "airpc-edge")
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.Edge.HTTPAddr)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("listen edge HTTP %s: %w", cfg.Edge.HTTPAddr, err)
	}

	routes := httpRoutes(cfg.Routes)
	server := &Server{
		httpServer: &http.Server{Handler: handler{nats: nc, routes: routes}},
		listener:   listener,
		nats:       nc,
		done:       make(chan error, 1),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.httpServer.Shutdown(shutdownCtx)
	}()
	go func() {
		err := server.httpServer.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		_ = nc.Drain()
		server.done <- err
		close(server.done)
	}()
	return server, nil
}

func Run(ctx context.Context, cfg config.Config, started io.Writer) error {
	server, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	if started != nil {
		fmt.Fprintf(started, "edge listening on http://%s with %d HTTP routes\n", server.Addr(), len(httpRoutes(cfg.Routes)))
	}
	return server.Wait()
}

func (s *Server) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Wait() error {
	if s == nil || s.done == nil {
		return nil
	}
	return <-s.done
}

func (s *Server) Close(ctx context.Context) error {
	if s == nil || s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

type handler struct {
	nats   *natscore.Client
	routes []route
}

func (h handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	matched, suffix, ok := matchRoute(h.routes, req)
	if !ok {
		http.NotFound(w, req)
		return
	}

	body, ok := readBounded(w, req.Body, routeRequestLimit(matched.cfg))
	if !ok {
		return
	}

	requestID, err := requestID()
	if err != nil {
		http.Error(w, "failed to create request id", http.StatusInternalServerError)
		return
	}
	timeout := routeTimeout(matched.cfg)
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()

	envelope := httpunary.Request{
		Version:        httpunary.Version,
		RequestID:      requestID,
		Route:          matched.cfg.Name,
		DeadlineUnixMS: deadline.UnixMilli(),
		Method:         req.Method,
		Scheme:         requestScheme(req),
		Authority:      req.Host,
		Path:           suffix,
		Headers:        httpheaders.FilterRequest(req.Header, matched.cfg.ForwardedHeaders),
		Body:           body,
	}
	payload, err := httpunary.EncodeRequest(envelope)
	if err != nil {
		http.Error(w, "invalid request envelope", http.StatusBadGateway)
		return
	}

	reply, err := h.nats.Request(ctx, matched.cfg.UnarySubject(), payload)
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, natscore.ErrNoResponders) {
			status = http.StatusServiceUnavailable
		} else if errors.Is(err, natscore.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, http.StatusText(status), status)
		return
	}

	resp, err := httpunary.DecodeResponse(reply)
	if err != nil || resp.RequestID != requestID || resp.Error != nil {
		http.Error(w, "connector error", http.StatusBadGateway)
		return
	}
	writeResponse(w, resp)
}

func httpRoutes(routes []config.Route) []route {
	out := make([]route, 0, len(routes))
	for _, r := range routes {
		if r.Mode != config.ModeHTTP {
			continue
		}
		out = append(out, route{cfg: r, prefix: r.PublicPrefix, exact: r.PublicPath})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return len(out[i].prefix)+len(out[i].exact) > len(out[j].prefix)+len(out[j].exact)
	})
	return out
}

func matchRoute(routes []route, req *http.Request) (route, string, bool) {
	for _, r := range routes {
		if !hostMatches(r.cfg.PublicHost, req.Host) {
			continue
		}
		if r.exact != "" && req.URL.Path == r.exact {
			return r, withQuery("/", req.URL.RawQuery), true
		}
		if r.prefix != "" && pathHasPrefix(req.URL.Path, r.prefix) {
			suffix := strings.TrimPrefix(req.URL.Path, r.prefix)
			if suffix == "" {
				suffix = "/"
			}
			return r, withQuery(suffix, req.URL.RawQuery), true
		}
	}
	return route{}, "", false
}

func hostMatches(routeHost, requestHost string) bool {
	if routeHost == "" {
		return true
	}
	return strings.EqualFold(normalizeHost(routeHost), normalizeHost(requestHost))
}

func normalizeHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func pathHasPrefix(path, prefix string) bool {
	if path == prefix {
		return true
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	return strings.HasSuffix(prefix, "/") || strings.HasPrefix(strings.TrimPrefix(path, prefix), "/")
}

func withQuery(path, rawQuery string) string {
	if rawQuery == "" {
		return path
	}
	return path + "?" + rawQuery
}

func requestScheme(req *http.Request) string {
	if req.TLS != nil {
		return "https"
	}
	if scheme := req.Header.Get("X-Forwarded-Proto"); scheme == "https" || scheme == "http" {
		return scheme
	}
	return "http"
}

func readBounded(w http.ResponseWriter, body io.ReadCloser, limit int64) ([]byte, bool) {
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return nil, false
	}
	if int64(len(data)) > limit {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return data, true
}

func routeRequestLimit(r config.Route) int64 {
	limit := int64(r.MaxInlineRequest())
	if limit <= 0 {
		return defaultRequestLimit
	}
	return limit
}

func routeTimeout(r config.Route) time.Duration {
	if r.Timeout.Duration > 0 {
		return r.Timeout.Duration
	}
	return defaultTimeout
}

func requestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func writeResponse(w http.ResponseWriter, resp httpunary.Response) {
	for name, values := range httpheaders.FilterResponse(resp.Headers) {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}
