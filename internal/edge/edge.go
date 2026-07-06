package edge

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/httpheaders"
	"github.com/terion-name/airpc/internal/natscore"
	"github.com/terion-name/airpc/internal/protocol/httpunary"
	"github.com/terion-name/airpc/internal/protocol/tunnel"
	"github.com/terion-name/airpc/internal/telemetry"
)

const (
	defaultRequestLimit = 1 << 20
	defaultTimeout      = 30 * time.Second
)

type Server struct {
	httpServer  *http.Server
	dataServer  *http.Server
	listener    net.Listener
	dataListen  net.Listener
	tcpListen   []net.Listener
	registry    *tunnelRegistry
	nats        *natscore.Client
	metricsAddr string
	done        chan error
}

type route struct {
	cfg    config.Route
	prefix string
	exact  string
}

func Start(ctx context.Context, cfg config.Config) (*Server, error) {
	nc, err := natscore.Connect(cfg.NATS.EdgeURLOrDefault(), "airpc-edge", cfg.NATS.CredsFile)
	if err != nil {
		return nil, err
	}
	metricsAddr, err := telemetry.StartServer(ctx, cfg.Edge.MetricsAddr)
	if err != nil {
		nc.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.Edge.HTTPAddr)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("listen edge HTTP %s: %w", cfg.Edge.HTTPAddr, err)
	}
	dataListener, err := net.Listen("tcp", cfg.Edge.DataAddr)
	if err != nil {
		_ = listener.Close()
		nc.Close()
		return nil, fmt.Errorf("listen edge data %s: %w", cfg.Edge.DataAddr, err)
	}
	var publicTLS *tls.Config
	if cfg.Edge.TLS != nil {
		publicTLS, err = cfg.Edge.TLS.ServerTLS()
		if err == nil {
			var dataTLS *tls.Config
			dataTLS, err = cfg.Edge.TLS.DataTLS()
			if err == nil {
				listener = tls.NewListener(listener, publicTLS)
				dataListener = tls.NewListener(dataListener, dataTLS)
			}
		}
		if err != nil {
			_ = listener.Close()
			_ = dataListener.Close()
			nc.Close()
			return nil, err
		}
	}

	registry := newTunnelRegistry()
	h := handler{nats: nc, httpRoutes: httpRoutes(cfg.Routes), wsRoutes: wsRoutes(cfg.Routes), registry: registry}
	server := &Server{
		httpServer:  &http.Server{Handler: h},
		dataServer:  &http.Server{Handler: dataHandler{registry: registry, token: cfg.Connector.TunnelToken}},
		listener:    listener,
		dataListen:  dataListener,
		registry:    registry,
		nats:        nc,
		metricsAddr: metricsAddr,
		done:        make(chan error, 1),
	}

	for _, r := range tcpRoutes(cfg.Routes) {
		tcpListener, err := net.Listen("tcp", r.Listen)
		if err != nil {
			_ = listener.Close()
			_ = dataListener.Close()
			closeListeners(server.tcpListen)
			nc.Close()
			return nil, fmt.Errorf("listen route %s on %s: %w", r.Name, r.Listen, err)
		}
		if r.TLS {
			tcpListener = tls.NewListener(tcpListener, publicTLS)
		}
		server.tcpListen = append(server.tcpListen, tcpListener)
	}

	errCh := make(chan error, len(server.tcpListen)+2)
	go serveHTTP(errCh, server.httpServer, listener)
	go serveHTTP(errCh, server.dataServer, dataListener)
	for i, r := range tcpRoutes(cfg.Routes) {
		routeCfg := r
		listener := server.tcpListen[i]
		go func() {
			errCh <- serveTCPRoute(ctx, nc, registry, routeCfg, listener)
		}()
	}

	go func() {
		var runErr error
		select {
		case <-ctx.Done():
		case runErr = <-errCh:
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeHTTPServer(shutdownCtx, server.httpServer)
		closeHTTPServer(shutdownCtx, server.dataServer)
		cancel()
		closeListeners(server.tcpListen)
		registry.closeAll()
		_ = nc.Drain()
		server.done <- runErr
		close(server.done)
	}()
	return server, nil
}

func serveHTTP(errCh chan<- error, server *http.Server, listener net.Listener) {
	err := server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		err = nil
	}
	errCh <- err
}

func Run(ctx context.Context, cfg config.Config, started io.Writer) error {
	server, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	if started != nil {
		fmt.Fprintf(started, "edge listening on http://%s (data ws://%s%s) with %d HTTP routes, %d WebSocket routes, and %d TCP/gRPC routes\n", server.Addr(), server.DataAddr(), dataTunnelPath, len(httpRoutes(cfg.Routes)), len(wsRoutes(cfg.Routes)), len(tcpRoutes(cfg.Routes)))
	}
	return server.Wait()
}

func (s *Server) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) DataAddr() string {
	if s == nil || s.dataListen == nil {
		return ""
	}
	return s.dataListen.Addr().String()
}

func (s *Server) MetricsAddr() string {
	if s == nil {
		return ""
	}
	return s.metricsAddr
}

func (s *Server) TCPAddrs() []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, len(s.tcpListen))
	for _, listener := range s.tcpListen {
		out = append(out, listener.Addr().String())
	}
	return out
}

func (s *Server) Wait() error {
	if s == nil || s.done == nil {
		return nil
	}
	return <-s.done
}

func (s *Server) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	closeListeners(s.tcpListen)
	closeHTTPServer(ctx, s.dataServer)
	closeHTTPServer(ctx, s.httpServer)
	if s.registry != nil {
		s.registry.closeAll()
	}
	return nil
}

type handler struct {
	nats       *natscore.Client
	httpRoutes []route
	wsRoutes   []route
	registry   *tunnelRegistry
}

func (h handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/_airpc/healthz" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
		return
	}

	if matched, suffix, ok := matchRoute(h.wsRoutes, req); ok {
		handleWebSocketRoute(req.Context(), h.nats, h.registry, matched, suffix, w, req)
		return
	}

	matched, suffix, ok := matchRoute(h.httpRoutes, req)
	if !ok {
		http.NotFound(w, req)
		return
	}
	h.serveUnary(w, req, matched, suffix)
}

// statusRecorder captures the response status for metrics while passing
// Flush through for streamed bodies.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (h handler) serveUnary(rw http.ResponseWriter, req *http.Request, matched route, suffix string) {
	start := time.Now()
	recorder := &statusRecorder{ResponseWriter: rw, status: http.StatusOK}
	var w http.ResponseWriter = recorder
	defer func() {
		telemetry.HTTPRequests.WithLabelValues(matched.cfg.Name, strconv.Itoa(recorder.status)).Inc()
		telemetry.HTTPDuration.WithLabelValues(matched.cfg.Name).Observe(time.Since(start).Seconds())
	}()

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
		if ctx.Err() != nil {
			// Client gone or deadline hit: the reply is unusable either way,
			// so tell connectors to abort the backend call.
			_ = h.nats.Publish(config.CancelSubject(requestID), nil)
		}
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
	if resp.StreamSessionID != "" {
		h.serveStreamedResponse(w, req, resp)
		return
	}
	writeResponse(w, resp)
}

// serveStreamedResponse relays a response body the connector streams over its
// data tunnel instead of inlining in the NATS reply. The stream is not bound
// by the route timeout; it ends with the body, a client disconnect, or a
// tunnel failure.
func (h handler) serveStreamedResponse(w http.ResponseWriter, req *http.Request, resp httpunary.Response) {
	link := h.registry.get(resp.ConnectorID)
	if link == nil {
		http.Error(w, "connector stream unavailable", http.StatusBadGateway)
		return
	}
	session := claimStreamSession(link, resp.StreamSessionID)
	if session == nil {
		http.Error(w, "connector stream unavailable", http.StatusBadGateway)
		return
	}
	defer session.Close()

	for name, values := range httpheaders.FilterResponse(resp.Headers) {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(resp.Status)
	flusher, _ := w.(http.Flusher)

	for {
		select {
		case <-req.Context().Done():
			return
		case <-session.Done():
			// Tunnel died mid-body: abort the connection so the client sees
			// a truncated response, not a clean end.
			panic(http.ErrAbortHandler)
		case frame := <-session.Recv():
			switch frame.Type {
			case tunnel.FrameData:
				if _, err := w.Write(frame.Payload); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				session.AckData()
			case tunnel.FrameEOF:
				return
			case tunnel.FrameClose, tunnel.FrameError:
				panic(http.ErrAbortHandler)
			}
		}
	}
}

func httpRoutes(routes []config.Route) []route {
	out := make([]route, 0, len(routes))
	for _, r := range routes {
		if r.Mode != config.ModeHTTP {
			continue
		}
		out = append(out, route{cfg: r, prefix: r.PublicPrefix, exact: r.PublicPath})
	}
	return sortRoutes(out)
}

func sortRoutes(routes []route) []route {
	sort.SliceStable(routes, func(i, j int) bool {
		return len(routes[i].prefix)+len(routes[i].exact) > len(routes[j].prefix)+len(routes[j].exact)
	})
	return routes
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
