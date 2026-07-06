# airpc

airpc is an outbound-only edge/connector gateway: expose private RPC services without opening a single inbound port on the private network. It supports:

- **HTTP** proxying over NATS Core request/reply, with SSE/chunked/large responses streamed over the data tunnel and client-disconnect cancellation propagated to the backend;
- **TCP** and **gRPC** opaque byte streams over a connector-initiated WebSocket data tunnel;
- **WebSocket** message relay (including close-code propagation) over the same tunnel;
- **TLS** termination on public listeners plus optional mTLS between connector and edge; and
- **Prometheus metrics**, including per-method gRPC observability decoded passively from the opaque relay.

An **edge** process listens on public HTTP/TCP addresses. A private **connector** process dials NATS, queue-subscribes for configured routes, and opens the outbound data WebSocket to the edge. HTTP requests use MessagePack envelopes on `airpc.v1.route.<route>.unary`; cancels are published on `airpc.v1.cancel.<request_id>`. Non-unary routes first use `airpc.v1.route.<route>.open` to select a connector, then relay frames on that connector's active tunnel.

SDK adapters, transparent proxying, and protocol-aware gRPC parsing are deliberately post-MVP (see `Known limitations`).

## Run locally

Start NATS, then run one edge and one connector with the same config:

```sh
nats-server -js=false

go run ./cmd/airpc edge start --config examples/airpc.yaml

go run ./cmd/airpc connector start --config examples/airpc.yaml --id local-1
```

With the example config, requests to the edge under `/demo` are sent to the connector and forwarded to `http://127.0.0.1:9000` with the public prefix stripped:

```sh
curl -i http://127.0.0.1:8080/demo/hello?name=airpc
```

The same edge process also starts:

- `edge.data_addr` for connector-owned WebSocket tunnels at `/_airpc/data`;
- each `tcp` route's `listen` address and relays raw bytes to the connector target; and
- each `grpc` route's `listen` address as an opaque HTTP/2/TCP stream.

WebSocket routes are served on the edge HTTP listener using `public_path` or `public_prefix` and are relayed as WebSocket messages to private `ws://` or `wss://` targets.

`grpc` mode intentionally preserves the raw HTTP/2/TCP stream — trailers, `*-bin` metadata, deadlines, cancellation, and bidirectional streaming all pass through untouched, because re-terminating gRPC is where proxies break in subtle ways. Protocol awareness is observation-only: see "Observability" below.

### HTTP unary, streaming responses, and cancellation

HTTP request bodies are read fully (bounded by `max_inline_request`) and carried in the NATS request. Responses are inlined in the NATS reply when they fit; when a response is chunked, an `text/event-stream`, or its declared length exceeds `max_inline_response`, the connector instead streams the body through an `http-stream` session on its data tunnel and the edge flushes chunks to the client as they arrive. Streamed bodies are not bound by the route `timeout` (which still bounds request dispatch and response headers). If the connector's tunnel is down, responses that fit inline still work; oversized ones fail.

When the public client disconnects or the route timeout expires before a reply, the edge publishes `airpc.v1.cancel.<request_id>` and the connector aborts the in-flight backend request.

### Data tunnel semantics

One WebSocket tunnel per connector multiplexes all stream sessions as MessagePack frames keyed by `session_id`.

- **Lossless, per-session flow control.** Each side may have at most 32 unacknowledged data frames (~1 MiB at the 32 KiB relay read size) in flight per session; the receiver returns credit with `window` frames as it consumes data. A slow client or backend backpressures only its own session — other sessions on the same tunnel keep flowing.
- **TCP half-close propagation.** A client (or backend) write-side `FIN` is relayed as an `eof` frame and half-closes the other end, so request/half-close/response protocols work; the session ends when both directions have finished.
- **Automatic tunnel reconnect.** The connector redials the edge data WebSocket with exponential backoff (250ms doubling to 5s) for the life of the process. While disconnected, the connector rejects route opens over NATS; the edge retries rejected opens every 50ms until the route timeout, so another connector in the queue group can take the session. Established sessions on a dropped tunnel are closed, not resumed. Edge shutdown closes tunnels explicitly so connectors notice immediately.
- **Idle timeout.** Routes may set `idle_timeout`; a stream session with no relayed data in either direction for that long is closed (default: no idle timeout). WebSocket ping/pong keepalives are answered by each hop but are not relayed and do not count as activity.
- **WebSocket close codes.** A close initiated by either end is relayed with its original status code and text, so clients and backends complete their close handshakes normally.

## Configuration

See [`examples/airpc.yaml`](examples/airpc.yaml). Important fields are:

- `nats.url`: default NATS server URL used by both edge and connector.
- `nats.edge_url` / `nats.connector_url` (optional): role-specific NATS URLs, useful for separate NATS users and permissions in Compose or production.
- `nats.creds_file` (optional): path to a NATS `.creds` file used for authentication. NATS connections reconnect indefinitely after the initial connect succeeds.
- `edge.http_addr`: HTTP listen address for HTTP unary and public WebSocket routes.
- `edge.data_addr`: WebSocket tunnel listen address for connector data sessions.
- `edge.metrics_addr` / `connector.metrics_addr` (optional): serve Prometheus metrics at `/metrics`. The endpoint is unauthenticated — bind it to a private interface.
- `edge.tls` (optional): `cert_file`/`key_file` terminate TLS on the HTTP and data listeners; `client_ca_file` additionally requires a verified connector client certificate on the **data listener only** (mTLS). TLS 1.2 minimum.
- `connector.edge_data_url`: connector URL for `edge.data_addr`, normally `ws://<edge-data>/_airpc/data` (`wss://` when `edge.tls` is set).
- `connector.tunnel_token` (optional): shared token required on the data tunnel.
- `connector.tls` (optional, requires a `wss://` data URL): `ca_file` trusts a private CA for the edge certificate; `cert_file`/`key_file` present a client certificate for mTLS; `server_name` overrides SNI.
- `routes[].name`: route token used in NATS subjects.
- `routes[].mode: http`: HTTP unary route. Uses `public_prefix`/`public_path`, URL `target`, inline body limits, `timeout`, and `forwarded_headers`.
- `routes[].mode: websocket`: public WebSocket route. Uses `public_prefix`/`public_path` and private `ws://`/`wss://` `target`.
- `routes[].mode: tcp`: public TCP listener. Uses `listen` and private `host:port` `target`.
- `routes[].mode: grpc`: public TCP listener for opaque gRPC-over-HTTP/2. Uses `listen` and private `host:port` `target`.
- `routes[].idle_timeout` (optional, stream modes): close a session after this long with no relayed data in either direction. Unset or `0` disables the idle check. Do not set it below the application's own keepalive interval — WebSocket ping/pong does not count as activity.
- `routes[].tls` (optional, tcp/grpc only): serve this route's public listener with the `edge.tls` certificate. Leave unset for TLS-passthrough of protocols that bring their own TLS.

Derived NATS names:

- Unary subject: `airpc.v1.route.<route>.unary`
- Open subject for stream routes: `airpc.v1.route.<route>.open`
- Queue group: `airpc.route.<route>.connectors`
- Cancel subject: `airpc.v1.cancel.<request_id>`

The edge strips hop-by-hop HTTP headers and only forwards HTTP request headers listed in `forwarded_headers`. Response hop-by-hop headers are stripped before writing back to the client. The runtime does not log request/response bodies or Authorization values.

## Observability

With `metrics_addr` set, edge and connector expose Prometheus metrics:

- `airpc_http_requests_total{route,status}` and `airpc_http_request_duration_seconds{route}` — HTTP unary at the edge.
- `airpc_stream_sessions_total` / `airpc_stream_sessions_active{route,mode}` and `airpc_stream_bytes_total{route,direction}` — tcp/grpc/websocket sessions.
- `airpc_tunnel_connected{connector_id}` and `airpc_tunnel_dials_total{connector_id,outcome}` — connector data-tunnel health.
- `airpc_grpc_rpcs_total{route,method,code}` and `airpc_grpc_rpc_duration_seconds{route,method}` — per-method gRPC RPCs on `grpc` routes.

The gRPC metrics come from a **passive** HTTP/2 decoder on the relayed bytes: it extracts `:path` and `grpc-status` from frame headers without ever modifying, delaying, or re-terminating the stream. It is deliberately best-effort — if observation cannot keep up or the stream is not parseable plaintext HTTP/2 (e.g. TLS-passthrough where the client encrypts end-to-end), observation goes dark for that connection and traffic is unaffected. Bodies, header values, and message payloads are never recorded.

## Docker Compose bench

The Compose bench in [`deploy/compose.yaml`](deploy/compose.yaml) runs NATS, edge, connector, private HTTP/TCP/WebSocket/gRPC backends, and a public-only test runner on separated `public`, `broker`, and internal `private` networks. Run:

```sh
make e2e
```

The smoke flow checks normal traffic, public/edge inability to reach private backend addresses directly, connector-down failures, and recovery after restarting the connector.

## Kubernetes

[`deploy/k8s/`](deploy/k8s/) contains plain manifests: a ConfigMap holding `airpc.yaml`, an edge Deployment + Service (readiness on `/_airpc/healthz`), and a connector Deployment whose replicas join the route queue groups using their pod names as connector IDs. Edit the ConfigMap's routes and targets, point `connector.edge_data_url` at the edge Service (or its public address when connectors run in another cluster), and:

```sh
kubectl apply -f deploy/k8s/
```

For production, move `tunnel_token` (or the mTLS key material) into a Secret and expose the edge via LoadBalancer or an Ingress in front of the `http` port.

## Known limitations

These are deliberate post-MVP cuts, in line with the original design's build order:

- **HTTP request bodies are inline-only.** Request bodies are bounded by `max_inline_request`; client-streaming and bidirectional HTTP (e.g. gRPC over the `http` mode) are not supported — use the opaque `grpc`/`tcp` modes for those. Response trailers are not relayed.
- **No gRPC re-termination.** gRPC is relayed opaquely by design; airpc observes methods/statuses for metrics but does not route per-method or rewrite gRPC semantics. Put a gRPC-aware L7 proxy (e.g. Envoy) in front of or behind airpc if per-method routing is required.
- **No SDK adapters, forward/transparent proxy modes, or Kubernetes operator.** Endpoint replacement (URL/host:port/DNS) is the supported integration path; the manifests in `deploy/k8s/` are static.

## Development

```sh
go test ./...
```

`internal/integration` starts an in-process NATS server, edge, and connector, and covers the full surface end-to-end: HTTP unary, response streaming (SSE incrementality, oversized bodies, inline fallback), client-disconnect cancellation, lossless bulk transfer under backpressure, TCP half-close, per-session isolation of a stalled session, idle teardown, tunnel recovery after an edge restart, open retry past a tunnel-down connector, WebSocket close-code propagation, TLS/mTLS (HTTPS unary, TLS TCP route, wss tunnel with client certificates, rejection without one), and Prometheus metrics including gRPC method/status observation of a real grpc-go call through the relay.
