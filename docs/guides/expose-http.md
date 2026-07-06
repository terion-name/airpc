# Guide: Expose an HTTP API

Works for anything that speaks HTTP request/response: REST, JSON-RPC, tRPC queries/mutations, Connect unary, Twirp, GraphQL, SSE endpoints, plain internal APIs. Clients only change their base URL.

## Minimal setup

`airpc.yaml`:

```yaml
nats:
  url: nats://127.0.0.1:4222
edge:
  http_addr: :8080
  data_addr: :8081
connector:
  edge_data_url: ws://127.0.0.1:8081/_airpc/data
routes:
  - name: api
    mode: http
    public_prefix: /api
    target: http://127.0.0.1:9000
    forwarded_headers: [authorization, content-type, accept]
```

Run all three pieces (NATS, edge, connector) and test:

```sh
nats-server -js=false &
airpc edge start --config airpc.yaml &
airpc connector start --config airpc.yaml --id local-1 &

# private demo backend
python3 -m http.server 9000 &

curl -i http://127.0.0.1:8080/api/
```

`GET /api/anything?x=1` on the edge becomes `GET /anything?x=1` against the target — the public prefix is stripped, the query string is preserved.

## Routing rules

- **`public_prefix`** matches and strips a path prefix. When several routes could match, the **longest prefix wins**, so `/api/admin` can go to a different backend than `/api`.
- **`public_path`** matches one exact path and forwards it as `/` — good for single-endpoint RPC servers (`/rpc`).
- **`public_host`** additionally restricts a route to one `Host` header (port ignored), so many APIs can share the edge listener as virtual hosts:

```yaml
routes:
  - name: users
    mode: http
    public_host: users.example.com
    public_prefix: /
    target: http://users.internal:8080
  - name: billing
    mode: http
    public_host: billing.example.com
    public_prefix: /
    target: http://billing.internal:8080
```

- The target may carry a base path: with `target: http://svc.internal:8080/rpc` and `public_path: /users-rpc`, public calls to `/users-rpc` hit `/rpc` on the backend.

## Headers are allowlisted — don't skip this

Only headers listed in `forwarded_headers` reach the backend. **This includes `Authorization`** — if your API authenticates, list it explicitly:

```yaml
    forwarded_headers: [authorization, content-type, accept, traceparent, x-request-id]
```

Hop-by-hop headers (`Connection`, `Transfer-Encoding`, …) are always stripped in both directions, and response headers come back unfiltered except for those.

## Body limits and streaming

- Request bodies are read fully, capped by `max_inline_request` (default 1 MiB → `413` above that).
- Responses up to `max_inline_response` (default 16 MiB) return in a single NATS round trip.
- Responses that are **chunked, `text/event-stream`, or larger than the limit stream automatically** through the connector's data tunnel — the client sees a normal streamed HTTP response, flushed chunk by chunk, with backpressure end to end. No configuration needed.

SSE example — this works as-is, with events delivered as the backend emits them:

```sh
curl -N http://127.0.0.1:8080/api/events
```

The route `timeout` (default 30s) bounds dispatch and response headers; a streamed body may run as long as it likes.

## Timeouts and cancellation

- `timeout` is the whole-request budget for inline responses. On expiry the client gets `504` and the backend request is **canceled**, not abandoned.
- If the client disconnects mid-request, the edge publishes a cancel and the connector aborts the backend call — long-polling and slow endpoints don't leak backend work.

## Status codes you'll see from the edge

| Code | Meaning                                                                |
| ---- | ---------------------------------------------------------------------- |
| 404  | No route matched host/path                                              |
| 413  | Request body exceeded `max_inline_request`                              |
| 502  | Backend/connector error (bad envelope, backend refused, stream failed)  |
| 503  | No connector subscribed to the route                                    |
| 504  | Route `timeout` elapsed                                                 |

## Not supported (by design, for `http` mode)

Client-streaming and bidirectional HTTP bodies, and response trailers. Protocols that need those (gRPC!) should use the [`grpc` route mode](expose-grpc.md), which relays them opaquely and loses nothing.
