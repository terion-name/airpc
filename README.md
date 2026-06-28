# airpc

airpc is an outbound-only RPC/socket gateway scaffold. A public **edge** will accept client traffic, while a private-side **connector** will make outbound-only connections to NATS and to the edge's tunnel endpoint. NATS Core is the control plane for unary request/reply and opaque session open messages. A connector-initiated WebSocket tunnel is the planned data plane for TCP, WebSocket, and gRPC-ish opaque streams.

Wire protocols are binary, closed-schema MessagePack envelopes. There are no JSON control or data protocols.

## Current status

This repository contains compile-able foundations only:

- plan-shaped YAML config loading and validation for edge and connector roles
- safe route/connector tokens and NATS subject builders
- HTTP header filtering for safe forwarding
- MessagePack unary/open envelopes and tunnel frame codecs
- a small NATS Core wrapper with typed error mapping
- a thin stdlib `flag` CLI contract

The edge/connector runtime networking, tunnel relay, and request handling loops are not wired yet.

## Quickstart shape

```sh
go test ./...
go run ./cmd/airpc validate --config examples/airpc.yaml
```

Runtime commands validate the config and then return a clear not-yet-wired error until the runtime lands:

```sh
go run ./cmd/airpc edge start --config examples/airpc.yaml
go run ./cmd/airpc connector start --config examples/airpc.yaml --id local-1
```

## Config shape

See [`examples/airpc.yaml`](examples/airpc.yaml) for a complete plan-shaped file. The top-level sections are:

- `nats.urls`: one or more `nats://`, `tls://`, `ws://`, or `wss://` URLs. Singular `nats.url` is accepted for compatibility.
- `defaults.request_timeout`, `defaults.max_request_bytes`, `defaults.max_response_bytes`: safe global defaults used by routes.
- `edge.http.listen`: public HTTP listen address for unary HTTP, public WebSocket matching, and the tunnel endpoint.
- `edge.http.tunnel_path`: connector WebSocket tunnel path, defaulting to `/_airpc/v1/tunnel` when omitted.
- `edge.routes[]`: public route match/listen definitions.
- `connector.id`: default connector ID, overrideable with `connector start --id ...`.
- `connector.edge_tunnel_url`: outbound WebSocket tunnel URL used by the connector.
- `connector.routes[]`: private targets this connector can reach.

Route types are `http`, `tcp`, `websocket`, and `grpc`.

Edge HTTP/WebSocket routes use `hosts`, `path_prefix`, and `strip_prefix`. Edge TCP/gRPC routes use `listen`. Connector HTTP targets must be `http://` or `https://`; WebSocket targets must be `ws://` or `wss://`; TCP/gRPC targets must be `host:port`.

Route names and connector IDs must be safe NATS token paths: bounded, non-empty, no wildcards, no blank/control characters, and no empty `.` tokens. URL targets reject embedded credentials.

Header allowlists use `request_headers` and `response_headers`. Names are normalized through `internal/headerx`; hop-by-hop and sensitive headers are consistently excluded.

## Protocol foundations

Unary and open control messages are MessagePack arrays with exact lengths and version checks. Open requests carry route, request/session IDs, session kind (`tcp`, `websocket`, `grpc`), deadline Unix milliseconds, public host/path/query, headers, and trace context fields. Open responses carry route, request/session IDs, selected connector ID, accept/reject state, and error code/message.

Tunnel frames are also closed-schema MessagePack arrays with version, frame type (`OPEN`, `OPEN_OK`, `OPEN_ERR`, `DATA`, `CLOSE`, `PING`, `PONG`), session ID, kind, flags, opcode, payload, close code, and reason. Decode enforces max frame/payload size and rejects wrong versions, wrong lengths, trailing data, invalid sessions, invalid kinds, and invalid opcodes.
