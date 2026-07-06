# Configuration reference

Both processes read the same YAML file — the edge uses the `edge` and `routes` sections, the connector uses `connector` and `routes`, and both use `nats`. Sharing one file keeps the two sides' view of routes consistent; in production you typically render two variants of it (the connector one contains private targets, the edge one doesn't need them — but shipping identical files is also fine, since the edge never dials targets).

Unknown fields are rejected, and the whole file is validated before anything starts, so typos fail fast at startup.

```sh
airpc edge start --config airpc.yaml
airpc connector start --config airpc.yaml --id connector-1
```

The connector `--id` must be unique per instance (lowercase pod names work well) and is a strict token: `[A-Za-z0-9_-]`, max 128 characters.

## Value syntax

- **Durations** use Go syntax: `300ms`, `5s`, `10m`, `1h`.
- **Byte sizes** accept a plain number (bytes) or a unit suffix, case-insensitive: decimal `kb`, `mb`, `gb` and binary `kib`, `mib`, `gib` — e.g. `512KiB`, `4MiB`.
- **Address fields** are `host:port`; the host may be empty to bind all interfaces (`:8080`).

## `nats`

| Field           | Required | Description                                                                                        |
| --------------- | -------- | -------------------------------------------------------------------------------------------------- |
| `url`           | yes      | NATS server URL. Schemes: `nats://`, `tls://`, `ws://`, `wss://`.                                   |
| `edge_url`      | no       | Overrides `url` for the edge — use it to give edge and connector separate NATS users/permissions.   |
| `connector_url` | no       | Overrides `url` for the connector.                                                                  |
| `creds_file`    | no       | Path to a NATS `.creds` file for JWT/NKey authentication.                                           |

The initial NATS connect fails fast (misconfiguration surfaces at startup); after that, reconnects are unlimited.

Giving the two roles separate NATS accounts is recommended in production: the edge only needs to *publish* to `airpc.v1.route.*` / `airpc.v1.cancel.*` and receive replies, the connector only needs to *subscribe* to them. See [`deploy/nats/nats.conf`](../deploy/nats/nats.conf) for a working two-user example.

## `edge`

| Field          | Required | Description                                                                     |
| -------------- | -------- | -------------------------------------------------------------------------------- |
| `http_addr`    | yes      | Public listener for `http` and `websocket` routes. Also serves `/_airpc/healthz`.|
| `data_addr`    | yes      | Listener for connector data tunnels (WebSocket at `/_airpc/data`). Reachable by connectors, not necessarily by the public. |
| `metrics_addr` | no       | Prometheus `/metrics`. Unauthenticated — bind to a private interface.            |
| `tls`          | no       | Terminate TLS on the HTTP and data listeners (below).                            |

### `edge.tls`

| Field            | Required        | Description                                                                                     |
| ---------------- | --------------- | ------------------------------------------------------------------------------------------------ |
| `cert_file`      | yes (with key)  | PEM certificate served on the HTTP listener, the data listener, and any `tls: true` TCP route.    |
| `key_file`       | yes (with cert) | PEM private key.                                                                                  |
| `client_ca_file` | no              | PEM CA bundle. When set, the **data listener only** requires and verifies connector client certificates (mTLS). Public listeners never request client certificates. |

TLS 1.2 is the minimum version everywhere.

## `connector`

| Field           | Required | Description                                                                                          |
| --------------- | -------- | ----------------------------------------------------------------------------------------------------- |
| `edge_data_url` | yes      | Where to dial the edge's data listener: `ws://<edge-data-host:port>/_airpc/data`, or `wss://…` with `edge.tls`. Must not embed credentials. |
| `tunnel_token`  | no       | Shared secret required on the tunnel. Sent as `Authorization: Bearer`; the edge also accepts it as a `token` query parameter. Use a Secret, not a literal, in real deployments. |
| `metrics_addr`  | no       | Prometheus `/metrics` for the connector process.                                                       |
| `tls`           | no       | TLS settings for a `wss://` dial (below). Requires `edge_data_url` to be `wss://`.                     |

### `connector.tls`

| Field         | Required        | Description                                                          |
| ------------- | --------------- | --------------------------------------------------------------------- |
| `ca_file`     | no              | PEM CA bundle to trust for the edge certificate (private CA support). |
| `cert_file`   | with `key_file` | Client certificate presented to the edge (mTLS).                      |
| `key_file`    | with `cert_file`| Client private key.                                                   |
| `server_name` | no              | Overrides SNI / certificate hostname verification.                    |

## `routes[]`

Every route needs `name`, `mode`, and `target`. The rest depends on the mode.

### Common fields

| Field          | Applies to  | Default | Description                                                                                   |
| -------------- | ----------- | ------- | ---------------------------------------------------------------------------------------------- |
| `name`         | all         | —       | Route identifier, used in NATS subjects. Token: `[A-Za-z0-9_-]`, max 128 chars, unique.        |
| `mode`         | all         | —       | `http`, `websocket`, `tcp`, or `grpc`.                                                          |
| `target`       | all         | —       | Private destination. Scheme must match the mode (see below). Must not embed credentials.       |
| `timeout`      | all         | `30s`   | HTTP: whole request budget (dispatch + response headers + inline body). Streams: session-open budget, including open retries. |
| `idle_timeout` | stream modes| off     | Close a session after this long with no relayed data in either direction. Don't set it below the application's own keepalive interval — WebSocket ping/pong doesn't count as activity. |

### `mode: http`

Proxied over NATS request/reply, with automatic tunnel streaming for large/SSE/chunked responses.

| Field                 | Default | Description                                                                                       |
| --------------------- | ------- | --------------------------------------------------------------------------------------------------|
| `target`              | —       | `http://` or `https://` base URL. The matched public suffix is appended to its path.               |
| `public_host`         | any     | Only match requests with this `Host` (port ignored, case-insensitive). Lets several routes share the listener by virtual host. |
| `public_prefix`       | —       | Match and strip a path prefix. Longest prefix wins across routes.                                  |
| `public_path`         | —       | Match one exact path (forwarded to the target as `/`). One of `public_prefix`/`public_path` is required. |
| `forwarded_headers`   | none    | **Allowlist** of request headers forwarded to the backend. Everything else — including `Authorization` — is dropped unless listed. Hop-by-hop headers are always stripped. |
| `max_inline_request`  | `1MiB`  | Request body cap. Bigger requests get `413`.                                                       |
| `max_inline_response` | `16MiB` | Threshold above which responses stream through the data tunnel instead of the NATS reply.          |

(`max_request_body_bytes` / `max_response_body_bytes` are accepted as aliases for the two limits.)

### `mode: websocket`

Served on the edge HTTP listener; messages are relayed to a private WebSocket server.

| Field                             | Description                                                        |
| --------------------------------- | ------------------------------------------------------------------ |
| `target`                          | `ws://` or `wss://` base URL; the matched public suffix is appended.|
| `public_host` / `public_prefix` / `public_path` | Same matching rules as `http`.                       |

Text/binary messages and close codes are relayed; upgrade request headers and subprotocols are **not** forwarded to the backend.

### `mode: tcp` and `mode: grpc`

Each gets its own public TCP listener; bytes are relayed opaquely. `grpc` is behaviorally identical to `tcp` plus passive per-method metrics (see the [observability guide](guides/observability.md)).

| Field    | Default | Description                                                                                          |
| -------- | ------- | ------------------------------------------------------------------------------------------------------|
| `listen` | —       | Public `host:port` to listen on.                                                                      |
| `target` | —       | Private `host:port` to dial.                                                                          |
| `tls`    | `false` | Serve this listener with the `edge.tls` certificate. Leave off for TLS **passthrough** — clients that bring their own TLS (certificate pinning, strict mTLS to the backend) keep it end-to-end. |

## Derived names

For a route named `users`:

- Unary subject: `airpc.v1.route.users.unary`
- Stream-open subject: `airpc.v1.route.users.open`
- Queue group: `airpc.route.users.connectors`
- Cancels: `airpc.v1.cancel.<request_id>`

## Full annotated example

```yaml
nats:
  url: nats://nats.internal:4222
  # creds_file: /run/secrets/airpc.creds

edge:
  http_addr: :8443
  data_addr: :8444
  metrics_addr: 127.0.0.1:9090
  tls:
    cert_file: /certs/edge.crt
    key_file: /certs/edge.key
    client_ca_file: /certs/ca.crt      # connectors must present a client cert

connector:
  edge_data_url: wss://edge.example.com:8444/_airpc/data
  tunnel_token: replace-with-a-secret
  metrics_addr: 127.0.0.1:9090
  tls:
    ca_file: /certs/ca.crt             # trust the private CA for the edge cert
    cert_file: /certs/connector.crt    # mTLS identity
    key_file: /certs/connector.key

routes:
  # JSON-RPC / REST behind https://api.example.com/users/...
  - name: users
    mode: http
    public_host: api.example.com
    public_prefix: /users
    target: http://users.internal:8080
    forwarded_headers: [authorization, content-type, accept, x-request-id]
    max_inline_request: 4MiB
    max_inline_response: 4MiB          # bigger responses stream automatically
    timeout: 30s

  # Server-sent events; long-lived, so give it an idle guard
  - name: events
    mode: http
    public_prefix: /events
    target: http://events.internal:8080
    forwarded_headers: [accept, last-event-id]

  # gRPC, TLS terminated at the edge, backend speaks h2c
  - name: billing
    mode: grpc
    listen: :50051
    tls: true
    target: billing.internal:50051

  # Legacy Thrift socket — opaque passthrough
  - name: legacy-thrift
    mode: tcp
    listen: :9090
    target: thrift.internal:9090
    idle_timeout: 10m

  # Realtime subscriptions
  - name: realtime
    mode: websocket
    public_path: /ws
    target: ws://realtime.internal:3000/ws
```

## Validation rules worth knowing

- `routes[].tls` requires `edge.tls` and is only valid for `tcp`/`grpc`.
- `connector.tls` requires a `wss://` `edge_data_url`; `edge.tls` requires both `cert_file` and `key_file`.
- Targets and `edge_data_url` must not contain userinfo (credentials in URLs are rejected).
- All limits and timeouts must be non-negative; route names must be unique.
