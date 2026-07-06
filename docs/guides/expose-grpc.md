# Guide: Expose a gRPC service

`grpc` mode gives each service a dedicated public TCP listener and relays the HTTP/2 stream **byte-for-byte**. Nothing is re-terminated, so everything gRPC depends on survives the boundary: unary and all three streaming shapes, deadlines (`grpc-timeout`), cancellation (RST_STREAM), trailers, `grpc-status`, and `*-bin` metadata. Clients change only the address they dial.

## Minimal setup (h2c end to end)

```yaml
nats:
  url: nats://127.0.0.1:4222
edge:
  http_addr: :8080
  data_addr: :8081
connector:
  edge_data_url: ws://127.0.0.1:8081/_airpc/data
routes:
  - name: billing
    mode: grpc
    listen: :50051           # public listener on the edge
    target: 127.0.0.1:50052  # private gRPC server (h2c)
```

Client side — point your existing stub at the edge:

```go
// before: grpc.NewClient("billing.internal:50052", ...)
conn, err := grpc.NewClient("edge.example.com:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
```

Or with `grpcurl`:

```sh
grpcurl -plaintext edge.example.com:50051 list
```

## TLS options

You have three shapes; pick per route.

**1. Terminate at the edge, h2c to the backend** — the common production setup:

```yaml
edge:
  tls:
    cert_file: /certs/edge.crt   # cert for the hostname clients dial
    key_file: /certs/edge.key
routes:
  - name: billing
    mode: grpc
    listen: :50051
    tls: true                     # this listener serves edge.tls
    target: billing.internal:50051
```

Clients use normal TLS credentials (`grpcurl edge.example.com:50051 list`, no `-plaintext`).

**2. TLS passthrough** — leave `tls` off and let the client do TLS with the *backend's* certificate through the pipe. This preserves end-to-end mTLS and certificate pinning; the trade-off is that the client must be dialing a name the backend's certificate covers, and airpc's per-method metrics go dark (the stream is ciphertext to the edge).

**3. h2c everywhere** — fine inside trusted networks and for local development.

## Deadlines, cancellation, streaming

All carried natively by the relayed HTTP/2 stream — airpc adds nothing and removes nothing. Two knobs interact with long-lived streams:

- `timeout` (default 30s) bounds only **session establishment** (picking a connector, dialing the backend), not the stream's life.
- `idle_timeout` (off by default) will kill a stream with no traffic in either direction. gRPC keepalive pings are HTTP/2 frames, so they *do* count as traffic — but only set this if your streams are expected to be chatty.

## Per-method observability

On `grpc` routes with plaintext HTTP/2 visible to the edge (h2c, or `tls: true` termination), airpc passively decodes frame headers from the relayed bytes and exports:

```
airpc_grpc_rpcs_total{route="billing",method="/billing.Billing/Charge",code="0"}
airpc_grpc_rpc_duration_seconds{route="billing",method="/billing.Billing/Charge"}
```

This is observation only — the decoder reads a copy of the stream and can never delay or alter traffic. See the [observability guide](observability.md).

## What if I need per-method routing or auth?

That's deliberate scope airpc doesn't take on (re-terminating gRPC is where proxies develop subtle bugs). Compose instead: put a gRPC-aware L7 proxy (Envoy, etc.) *behind* the connector or *in front of* the edge listener, and let airpc do what it's for — crossing the network boundary with zero inbound ports.
