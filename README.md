# airpc

airpc is an outbound-only edge/connector gateway. In this slice it supports:

- **HTTP unary** proxying over NATS Core request/reply;
- **TCP** and **gRPC** opaque byte streams over a connector-initiated WebSocket data tunnel; and
- **WebSocket** message relay over the same data tunnel.

An **edge** process listens on public HTTP/TCP addresses. A private **connector** process dials NATS, queue-subscribes for configured routes, and opens the outbound data WebSocket to the edge. HTTP requests use MessagePack envelopes on `airpc.v1.route.<route>.unary`. Non-unary routes first use `airpc.v1.route.<route>.open` to select a connector, then relay frames on that connector's active tunnel.

PKI/mTLS, Kubernetes manifests, SDKs, transparent proxying, and protocol-aware gRPC parsing are not implemented in this slice.

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

First-product `grpc` mode intentionally preserves the raw HTTP/2/TCP stream. It does not yet parse gRPC methods, metrics, statuses, or trailers.

## Configuration

See [`examples/airpc.yaml`](examples/airpc.yaml). Important fields are:

- `nats.url`: default NATS server URL used by both edge and connector.
- `nats.edge_url` / `nats.connector_url` (optional): role-specific NATS URLs, useful for separate NATS users and permissions in Compose or production.
- `edge.http_addr`: HTTP listen address for HTTP unary and public WebSocket routes.
- `edge.data_addr`: WebSocket tunnel listen address for connector data sessions.
- `connector.edge_data_url`: connector URL for `edge.data_addr`, normally `ws://<edge-data>/_airpc/data`.
- `connector.tunnel_token` (optional): shared token required on the data tunnel.
- `routes[].name`: route token used in NATS subjects.
- `routes[].mode: http`: HTTP unary route. Uses `public_prefix`/`public_path`, URL `target`, inline body limits, `timeout`, and `forwarded_headers`.
- `routes[].mode: websocket`: public WebSocket route. Uses `public_prefix`/`public_path` and private `ws://`/`wss://` `target`.
- `routes[].mode: tcp`: public TCP listener. Uses `listen` and private `host:port` `target`.
- `routes[].mode: grpc`: public TCP listener for opaque gRPC-over-HTTP/2. Uses `listen` and private `host:port` `target`.

Derived NATS names:

- Unary subject: `airpc.v1.route.<route>.unary`
- Open subject for stream routes: `airpc.v1.route.<route>.open`
- Queue group: `airpc.route.<route>.connectors`

The edge strips hop-by-hop HTTP headers and only forwards HTTP request headers listed in `forwarded_headers`. Response hop-by-hop headers are stripped before writing back to the client. The runtime does not log request/response bodies or Authorization values.

## Docker Compose bench

The Compose bench in [`deploy/compose.yaml`](deploy/compose.yaml) runs NATS, edge, connector, private HTTP/TCP/WebSocket/gRPC backends, and a public-only test runner on separated `public`, `broker`, and internal `private` networks. Run:

```sh
make e2e
```

The smoke flow checks normal traffic, public/edge inability to reach private backend addresses directly, connector-down failures, and recovery after restarting the connector.

## Development

```sh
go test ./...
```
