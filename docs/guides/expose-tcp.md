# Guide: Expose raw TCP

`tcp` mode is the universal fallback: a public TCP listener whose bytes are relayed opaquely to a private `host:port`. If a protocol isn't HTTP-shaped — Thrift TSocket, RSocket TCP, Redis/Postgres-style wire protocols, legacy Java/.NET sockets, anything custom or unknown — this mode carries it without trying to understand it.

## Minimal setup

```yaml
nats:
  url: nats://127.0.0.1:4222
edge:
  http_addr: :8080
  data_addr: :8081
connector:
  edge_data_url: ws://127.0.0.1:8081/_airpc/data
routes:
  - name: legacy-thrift
    mode: tcp
    listen: :9090              # public listener on the edge
    target: 127.0.0.1:9090     # private service
```

Try it with netcat against any private TCP service:

```sh
nc edge.example.com 9090
```

Each accepted public connection becomes one tunnel session to one freshly dialed backend connection — connection-per-connection, no pooling surprises.

## Semantics you can rely on

- **Lossless, ordered, flow-controlled.** Bytes arrive exactly as sent; a slow reader on either side backpressures through the tunnel to the other side's socket, per session (~1 MiB in-flight window), without affecting other sessions.
- **Half-close works.** A client that sends its request and shuts down its write side (`FIN`) still receives the full response — request/half-close/response protocols behave exactly as over a direct connection.
- **Failure is a closed socket.** Backend unreachable, connector down, tunnel drop — the public connection closes. Clients with reconnect logic need nothing special.

## Long-lived and idle connections

Sessions live until a side closes them. For protocols where abandoned connections are common, set an idle guard:

```yaml
    idle_timeout: 10m    # close sessions with no bytes either way for 10 minutes
```

`timeout` (default 30s) only bounds session *establishment* — how long the edge will spend selecting a connector (with retries across the pool) and getting the session open.

## TLS

- **Passthrough (default):** the edge relays whatever the client sends — including a TLS handshake meant for the backend. End-to-end TLS, pinning, and mTLS to the backend all work; airpc never sees plaintext.
- **Edge termination:** set `tls: true` on the route (requires `edge.tls`) and the public listener speaks TLS with the edge certificate while the backend receives plaintext. Good for adding TLS to a legacy plaintext protocol.

## A note on gRPC

gRPC-over-TCP would work fine through `tcp` mode, but use [`mode: grpc`](expose-grpc.md) instead — it is the same opaque relay plus passive per-method metrics.
