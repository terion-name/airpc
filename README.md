# airpc: Zero-Inbound RPC & Socket Gateway

**airpc** exposes RPC services and raw sockets that live in a strictly private network — behind a firewall, NAT, or VPN — to public clients or other network zones **without opening a single inbound port** to the private side. Clients keep using their existing HTTP, gRPC, WebSocket, or TCP clients; they only change the endpoint they dial.

It is the sibling of [**air3**](https://github.com/terion-name/air3), which applies the same zero-inbound edge/connector concept to files and objects (S3). The two compose naturally: **airpc carries the calls, air3 carries the bytes** — use airpc for APIs, RPC, and sockets, and air3 when large objects need to cross the same boundary. A copy of air3 is vendored under [`references/air3`](references/air3) as a design reference.

## Why

Imagine a gRPC or JSON-RPC service deep inside your private infrastructure that a public app — or a service in another, less-trusted zone — needs to call.

Normally you would have to open an inbound port, set up a VPN, or place a reverse proxy in a DMZ with network reachability into the private zone. Every one of those creates an inbound path into the network you were trying to keep closed.

**airpc splits the responsibility instead:**

1. An **Edge Gateway** sits in the public zone or DMZ. It terminates client connections (HTTP/1.1, WebSocket, TLS, raw TCP, gRPC-over-HTTP/2) but has **no credentials for and no route into** the private network.
2. A **Private Connector** sits next to your service. It holds the private reachability but has **no inbound listeners** — it only dials out: to NATS, and to the edge's data port.
3. A **NATS broker** is the control plane between them: routing, connector selection, cancellation. Bulk data never flows through NATS.

```
Client / existing SDK
        │ HTTP/1.1 · gRPC · WebSocket · raw TCP
        ▼
  airpc-edge ───────── NATS control plane ─────────▶ airpc-connector
  public / DMZ                                        private network
        ▲                                                   │
        └────── connector-initiated data tunnel ◀───────────┘
                                                            ▼
                                              your existing RPC server
```

Because the connector initiates every connection, the private zone's firewall can stay **outbound-only**. Scaling is horizontal: connectors for the same route join a NATS queue group, and each request or session is handled by exactly one of them.

## What it carries

| Route mode  | Public side                                  | Private target        | Works for                                                        |
| ----------- | -------------------------------------------- | --------------------- | ---------------------------------------------------------------- |
| `http`      | Path prefix / exact path on the edge         | `http://` / `https://`| REST, JSON-RPC, tRPC, Connect unary, SSE, large downloads         |
| `grpc`      | Dedicated TCP listener (opaque HTTP/2)       | `host:port`           | gRPC — unary, server/client/bidi streaming, all metadata intact   |
| `websocket` | Path prefix / exact path on the edge         | `ws://` / `wss://`    | tRPC subscriptions, RSocket-over-WS, custom realtime protocols    |
| `tcp`       | Dedicated TCP listener                       | `host:port`           | Thrift TSocket, RSocket TCP, databases, any binary protocol       |

Key behaviors (details in [docs/architecture.md](docs/architecture.md)):

- **HTTP responses stream.** SSE, chunked, and oversized responses are streamed through the data tunnel with backpressure; small responses ride the NATS reply. Client disconnects cancel the backend request.
- **The data tunnel is lossless and fair.** Per-session credit-based flow control means one slow consumer never stalls other sessions; TCP half-close and WebSocket close codes propagate faithfully.
- **It heals itself.** Connectors redial the tunnel with backoff after an edge restart; the edge retries session opens across the connector pool; NATS reconnects are unlimited.
- **TLS where you want it.** The edge can terminate TLS on any listener; the tunnel supports mTLS with a private CA; or leave a TCP route as passthrough for end-to-end TLS.
- **Observable.** Prometheus metrics on both processes — including per-method gRPC method/status/latency decoded *passively* from the relayed stream, without ever re-terminating gRPC.

## Quick start

Requirements: Go 1.22+, a NATS server.

```sh
# 1. Start NATS (control plane)
nats-server -js=false

# 2. Start something private to expose (demo: a local HTTP server)
python3 -m http.server 9000

# 3. Start the edge and a connector with the same config
go run ./cmd/airpc edge start --config examples/airpc.yaml
go run ./cmd/airpc connector start --config examples/airpc.yaml --id local-1
```

The example config maps the public prefix `/demo` to the private `http://127.0.0.1:9000`:

```sh
curl -i http://127.0.0.1:8080/demo/
```

That's the whole model. Guides for each protocol and deployment topic:

## Documentation

- [Architecture](docs/architecture.md) — components, control plane vs. data plane, tunnel protocol, security model, failure behavior
- [Configuration reference](docs/configuration.md) — every field, defaults, and a fully annotated example
- Guides:
  - [Expose an HTTP API](docs/guides/expose-http.md) — routing, headers, body limits, streaming/SSE, cancellation
  - [Expose a gRPC service](docs/guides/expose-grpc.md) — opaque relay, TLS options, per-method metrics
  - [Expose a WebSocket service](docs/guides/expose-websocket.md)
  - [Expose raw TCP](docs/guides/expose-tcp.md) — Thrift, RSocket, databases, anything
  - [TLS & mTLS](docs/guides/tls.md) — terminate, passthrough, and lock the tunnel to a private CA
  - [Kubernetes](docs/guides/kubernetes.md) — manifests, scaling connectors, cross-cluster layout
  - [Observability](docs/guides/observability.md) — metrics reference and dashboards to build

## Deployment shapes

- **Local / bare metal:** one static binary (`go build ./cmd/airpc`), run `edge start` and `connector start` with a shared YAML file.
- **Docker Compose:** [`deploy/compose.yaml`](deploy/compose.yaml) runs the full topology — NATS, edge, connector, and HTTP/TCP/WebSocket/gRPC backends on segregated `public` / `broker` / `private` networks. `make e2e` builds it and runs the smoke suite, including connector-down and recovery scenarios.
- **Kubernetes:** [`deploy/k8s/`](deploy/k8s/) has ready-to-edit manifests; see the [Kubernetes guide](docs/guides/kubernetes.md).

## Relationship to air3

[air3](https://github.com/terion-name/air3) is the same security architecture applied to object storage: a public edge that validates signed URLs, a private connector that holds the S3 credentials, NATS carrying only small tickets, and the object bytes moving over a connector-initiated data plane. airpc generalizes that pattern from files to **calls and sockets**.

They are designed to be deployed side by side: point your service traffic at airpc and your file traffic at air3, and the private zone still has zero inbound ports. For request/response payloads too large to inline, the intended pattern is to keep the RPC on airpc and move the payload as an object reference through air3.

## Development

```sh
go test ./...          # unit + integration (in-process NATS, edge, connector)
go test -race ./...
make e2e               # Docker Compose end-to-end smoke
```

The integration suite covers the full surface end-to-end: HTTP unary and streaming (SSE incrementality, oversized bodies, inline fallback), cancellation, lossless bulk transfer under backpressure, TCP half-close, session isolation under a stalled peer, idle teardown, tunnel reconnect after an edge restart, open retry past a tunnel-down connector, WebSocket close codes, TLS/mTLS, and Prometheus metrics including passive gRPC observation of a real grpc-go call.

## Known limitations

Deliberate scope cuts, aligned with the original design's build order:

- **HTTP request bodies are inline-only** (bounded by `max_inline_request`); client-streaming/bidi HTTP is not supported — use the `grpc`/`tcp` modes, which carry it opaquely. HTTP response trailers are not relayed.
- **No gRPC re-termination.** gRPC is relayed byte-faithfully; airpc observes methods and statuses for metrics but does not route per-method or rewrite gRPC semantics. Put an L7 proxy (e.g. Envoy) in front of a route if you need that.
- **WebSocket request headers/subprotocols are not forwarded** to the private backend; the relay carries messages and close codes.
- **No SDK adapters, forward/transparent proxy modes, or Kubernetes operator.** Endpoint replacement (URL / host:port / DNS) is the supported integration path.
