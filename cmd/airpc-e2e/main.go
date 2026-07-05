package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	defaultBaseURL  = "http://edge:8080"
	defaultTCPAddr  = "edge:7000"
	defaultWSURL    = "ws://edge:8080/ws"
	defaultGRPCAddr = "edge:7003"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing command (backend, check, check-down, check-unreachable, wait-health)")
	}
	switch args[0] {
	case "backend":
		return runBackend(args[1:])
	case "check":
		return runCheck(args[1:], false)
	case "check-down":
		return runCheck(args[1:], true)
	case "check-unreachable":
		return runCheckUnreachable(args[1:])
	case "wait-health":
		return runWaitHealth(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runBackend(args []string) error {
	fs := flag.NewFlagSet("airpc-e2e backend", flag.ContinueOnError)
	mode := fs.String("mode", "", "backend mode: http, tcp, websocket, grpc")
	addr := fs.String("addr", "", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *mode == "" || *addr == "" {
		return fmt.Errorf("backend requires --mode and --addr")
	}

	switch *mode {
	case "http":
		return serveHTTPBackend(*addr)
	case "tcp":
		return serveTCPBackend(*addr)
	case "websocket":
		return serveWebSocketBackend(*addr)
	case "grpc":
		return serveGRPCBackend(*addr)
	default:
		return fmt.Errorf("unsupported backend mode %q", *mode)
	}
}

func runWaitHealth(args []string) error {
	fs := flag.NewFlagSet("airpc-e2e wait-health", flag.ContinueOnError)
	baseURL := fs.String("base-url", defaultBaseURL, "edge base URL")
	attempts := fs.Int("attempts", 60, "number of attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < *attempts; i++ {
		resp, err := client.Get(strings.TrimRight(*baseURL, "/") + "/_airpc/healthz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Println("ok: edge health is ready")
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("edge health did not become ready at %s", *baseURL)
}

func runCheck(args []string, down bool) error {
	fs := flag.NewFlagSet("airpc-e2e check", flag.ContinueOnError)
	baseURL := fs.String("base-url", defaultBaseURL, "edge base URL")
	tcpAddr := fs.String("tcp-addr", defaultTCPAddr, "edge TCP route address")
	wsURL := fs.String("ws-url", defaultWSURL, "edge WebSocket route URL")
	grpcAddr := fs.String("grpc-addr", defaultGRPCAddr, "edge gRPC route address")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if down {
		return runDownChecks(*baseURL, *tcpAddr, *wsURL, *grpcAddr)
	}
	checks := []struct {
		name string
		fn   func() error
	}{
		{name: "http unary", fn: func() error { return checkHTTP(*baseURL) }},
		{name: "websocket", fn: func() error { return checkWebSocket(*wsURL) }},
		{name: "tcp", fn: func() error { return checkTCP(*tcpAddr) }},
		{name: "grpc", fn: func() error { return checkGRPC(*grpcAddr) }},
	}
	for _, check := range checks {
		if err := check.fn(); err != nil {
			return fmt.Errorf("%s check failed: %w", check.name, err)
		}
		fmt.Println("ok:", check.name)
	}
	return nil
}

func runDownChecks(baseURL, tcpAddr, wsURL, grpcAddr string) error {
	checks := []struct {
		name string
		fn   func() error
	}{
		{name: "http down", fn: func() error { return checkHTTPDown(baseURL) }},
		{name: "websocket down", fn: func() error { return checkWebSocketDown(wsURL) }},
		{name: "tcp down", fn: func() error { return checkTCPDown(tcpAddr) }},
		{name: "grpc down", fn: func() error { return checkGRPCDown(grpcAddr) }},
	}
	for _, check := range checks {
		if err := check.fn(); err != nil {
			return fmt.Errorf("%s check failed: %w", check.name, err)
		}
		fmt.Println("ok:", check.name)
	}
	return nil
}

func serveHTTPBackend(addr string) error {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		if r.URL.Path != "/api/echo" || r.URL.RawQuery != "x=1&y=two" {
			http.Error(w, "unexpected path or query: "+r.URL.RequestURI(), http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodPost || string(body) != "hello-http" {
			http.Error(w, "unexpected method or body", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-Airpc-Test") != "allowed" || r.Header.Get("X-Blocked") != "" || r.Header.Get("Authorization") != "" {
			http.Error(w, "unexpected forwarded headers", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Backend-Header", "preserved")
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, "http-ok:%s:%s:%s", r.URL.RequestURI(), body, r.Header.Get("X-Airpc-Test"))
	})
	log.Printf("http backend listening on %s", addr)
	return http.ListenAndServe(addr, h)
}

func serveTCPBackend(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("tcp backend listening on %s", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
	}
}

func serveWebSocketBackend(addr string) error {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
				continue
			}
			if err := conn.WriteMessage(messageType, payload); err != nil {
				return
			}
		}
	})
	log.Printf("websocket backend listening on %s", addr)
	return http.ListenAndServe(addr, h)
}

type grpcEchoService interface {
	Unary(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	Chat(grpc.ServerStream) error
}

type grpcBackend struct{}

func (grpcBackend) Unary(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("grpc-unary:" + req.GetValue()), nil
}

func (grpcBackend) Chat(stream grpc.ServerStream) error {
	for {
		var msg wrapperspb.StringValue
		if err := stream.RecvMsg(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stream.SendMsg(wrapperspb.String("grpc-stream:" + msg.GetValue())); err != nil {
			return err
		}
	}
}

func serveGRPCBackend(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "airpc.e2e.Echo",
		HandlerType: (*grpcEchoService)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Unary",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				var req wrapperspb.StringValue
				if err := dec(&req); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(grpcEchoService).Unary(ctx, &req)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/airpc.e2e.Echo/Unary"}
				handler := func(ctx context.Context, request any) (any, error) {
					return srv.(grpcEchoService).Unary(ctx, request.(*wrapperspb.StringValue))
				}
				return interceptor(ctx, &req, info, handler)
			},
		}},
		Streams: []grpc.StreamDesc{{
			StreamName:    "Chat",
			ServerStreams: true,
			ClientStreams: true,
			Handler: func(srv any, stream grpc.ServerStream) error {
				return srv.(grpcEchoService).Chat(stream)
			},
		}},
	}, grpcBackend{})
	log.Printf("grpc backend listening on %s", addr)
	return server.Serve(listener)
}

func checkHTTP(baseURL string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/http/api/echo?x=1&y=two", strings.NewReader("hello-http"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Airpc-Test", "allowed")
	req.Header.Set("X-Blocked", "must-not-forward")
	req.Header.Set("Authorization", "must-not-forward")
	req.Header.Set("Connection", "keep-alive")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("HTTP status = %d body = %q", resp.StatusCode, body)
	}
	wantBody := "http-ok:/api/echo?x=1&y=two:hello-http:allowed"
	if string(body) != wantBody {
		return fmt.Errorf("HTTP body = %q, want %q", body, wantBody)
	}
	if resp.Header.Get("X-Backend-Header") != "preserved" {
		return fmt.Errorf("X-Backend-Header = %q", resp.Header.Get("X-Backend-Header"))
	}
	if resp.Header.Get("Connection") != "" {
		return fmt.Errorf("hop-by-hop Connection header was forwarded")
	}
	tooLargeReq, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/http/api/echo?x=1&y=two", strings.NewReader(strings.Repeat("x", 128)))
	if err != nil {
		return err
	}
	tooLargeResp, err := client.Do(tooLargeReq)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, tooLargeResp.Body)
	tooLargeResp.Body.Close()
	if tooLargeResp.StatusCode != http.StatusRequestEntityTooLarge {
		return fmt.Errorf("large request status = %d", tooLargeResp.StatusCode)
	}
	return nil
}

func checkHTTPDown(baseURL string) error {
	client := &http.Client{Timeout: 6 * time.Second}
	resp, err := client.Post(strings.TrimRight(baseURL, "/")+"/http/api/echo?x=1&y=two", "text/plain", strings.NewReader("hello-http"))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return nil
	default:
		return fmt.Errorf("HTTP status = %d, want unavailable/timeout", resp.StatusCode)
	}
}

func checkWebSocket(rawURL string) error {
	conn, _, err := websocket.DefaultDialer.Dial(rawURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	messages := []struct {
		kind int
		data []byte
	}{
		{kind: websocket.TextMessage, data: []byte("hello text")},
		{kind: websocket.BinaryMessage, data: []byte{0, 1, 2, 3, 255}},
		{kind: websocket.TextMessage, data: []byte("second text")},
	}
	for _, msg := range messages {
		if err := conn.WriteMessage(msg.kind, msg.data); err != nil {
			return err
		}
		kind, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if kind != msg.kind || !bytes.Equal(data, msg.data) {
			return fmt.Errorf("websocket echo kind=%d data=%v", kind, data)
		}
	}
	return nil
}

func checkWebSocketDown(rawURL string) error {
	conn, _, err := websocket.DefaultDialer.Dial(rawURL, nil)
	if err != nil {
		return nil
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(6 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte("should-close")); err != nil {
		return nil
	}
	_, _, err = conn.ReadMessage()
	if err != nil {
		return nil
	}
	return fmt.Errorf("websocket stayed open while connector was down")
}

func checkTCP(addr string) error {
	if err := checkTCPSession(addr, []byte("first")); err != nil {
		return err
	}
	large := make([]byte, 64*1024)
	if _, err := rand.Read(large); err != nil {
		return err
	}
	if err := checkTCPSession(addr, large); err != nil {
		return err
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("concurrent-%d", i))
			errs <- checkTCPSession(addr, payload)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func checkTCPSession(addr string, first []byte) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeAndReadTCP(conn, first); err != nil {
		return err
	}
	return writeAndReadTCP(conn, []byte("second-write"))
}

func writeAndReadTCP(conn net.Conn, payload []byte) error {
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		return err
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("tcp echo mismatch: got %d bytes", len(got))
	}
	return nil
}

func checkTCPDown(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(6 * time.Second))
	if _, err := conn.Write([]byte("down")); err != nil {
		return nil
	}
	buf := make([]byte, 4)
	_, err = conn.Read(buf)
	if err != nil {
		return nil
	}
	return fmt.Errorf("tcp route echoed data while connector was down")
}

func checkGRPC(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()
	var unaryResp wrapperspb.StringValue
	if err := conn.Invoke(ctx, "/airpc.e2e.Echo/Unary", wrapperspb.String("payload"), &unaryResp); err != nil {
		return err
	}
	if unaryResp.GetValue() != "grpc-unary:payload" {
		return fmt.Errorf("grpc unary response = %q", unaryResp.GetValue())
	}
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, "/airpc.e2e.Echo/Chat")
	if err != nil {
		return err
	}
	for _, value := range []string{"one", "two", "three"} {
		if err := stream.SendMsg(wrapperspb.String(value)); err != nil {
			return err
		}
		var resp wrapperspb.StringValue
		if err := stream.RecvMsg(&resp); err != nil {
			return err
		}
		if resp.GetValue() != "grpc-stream:"+value {
			return fmt.Errorf("grpc stream response = %q", resp.GetValue())
		}
	}
	return stream.CloseSend()
}

func checkGRPCDown(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil
	}
	defer conn.Close()
	var resp wrapperspb.StringValue
	err = conn.Invoke(ctx, "/airpc.e2e.Echo/Unary", wrapperspb.String("payload"), &resp)
	if err != nil {
		return nil
	}
	return fmt.Errorf("grpc route succeeded while connector was down")
}

func runCheckUnreachable(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("check-unreachable requires at least one host:port")
	}
	for _, addr := range args {
		if err := checkUnreachable(addr); err != nil {
			return err
		}
		fmt.Println("ok: unreachable", addr)
	}
	return nil
}

func checkUnreachable(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil
	}
	conn.Close()
	return fmt.Errorf("%s was reachable", addr)
}
