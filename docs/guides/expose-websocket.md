# Guide: Expose a WebSocket service

`websocket` routes live on the edge's HTTP listener (sharing it with `http` routes) and relay messages to a private WebSocket server. Works for tRPC subscriptions, RSocket-over-WebSocket, GraphQL subscriptions, and custom realtime protocols.

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
  - name: realtime
    mode: websocket
    public_path: /ws
    target: ws://127.0.0.1:3000/ws
```

Clients change only the URL:

```js
// before: new WebSocket("ws://realtime.internal:3000/ws")
const ws = new WebSocket("wss://edge.example.com/ws");
```

With `public_prefix` instead of `public_path`, the matched suffix (and query string) is appended to the target path — so `wss://edge/chat/room/42?a=1` with `public_prefix: /chat` and `target: ws://chat.internal:3000` dials `ws://chat.internal:3000/room/42?a=1`.

## What is relayed — and what isn't

Relayed faithfully:

- **Text and binary messages**, in order, with per-session flow control (a slow reader backpressures its own session only).
- **Close codes and reasons**, in both directions — a backend closing with `4001 policy` delivers exactly that to the client, so application close semantics survive.

Handled hop-by-hop (not relayed):

- **Ping/pong** keepalives — each hop answers its own. Consequence: pings do **not** count as session activity, so don't set `idle_timeout` below your application's own message cadence.

Not forwarded (current limitation):

- **Upgrade request headers and subprotocols** (`Sec-WebSocket-Protocol`, cookies, `Authorization`). If your backend authenticates the upgrade, carry the credential in the URL query or the first message for now.

## Behavior under failure

- No connector / tunnel down: the upgrade succeeds and is immediately closed with `1013 (try again later)` after the open times out (`timeout`, default 30s, with retries across the connector pool).
- Edge restart: sessions drop; clients should reconnect (they'll land on a redialed tunnel within ~250 ms).

## TLS

`wss://` for the public side comes from `edge.tls` (it covers the whole HTTP listener). A `wss://` *target* makes the connector do TLS to the private backend with system roots.
