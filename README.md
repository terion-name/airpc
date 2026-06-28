# airpc

airpc is an early scaffold for an outbound-only edge/connector gateway. The intended architecture is:

- an **edge** process listens on public HTTP and data addresses;
- a private **connector** dials out to the edge data endpoint;
- NATS subjects are planned for route control messages; and
- routes describe public HTTP/WebSocket paths or TCP/gRPC listeners and their private targets.

This first slice does **not** implement NATS clients, MessagePack protocols, proxying, tunnels, TCP, WebSocket, or gRPC runtime networking.

## Current CLI status

The CLI only loads and validates YAML config, then prints what would start:

```sh
go run ./cmd/airpc edge start --config examples/airpc.yaml
go run ./cmd/airpc connector start --config examples/airpc.yaml --id local-1
```

Both commands return clear errors for missing `--config`; connector start also requires `--id`.

## Configuration

See [`examples/airpc.yaml`](examples/airpc.yaml). The current config includes:

- `nats.url`
- `edge.http_addr` and `edge.data_addr`
- `connector.edge_data_url` and optional `connector.tunnel_token`
- `routes[]` with `name`, `mode`, public path/listener fields, target, optional body limits, timeout, and forwarded headers

Route modes are `http`, `grpc`, `websocket`, and `tcp`. HTTP targets use `http://` or `https://`; WebSocket targets use `ws://` or `wss://`; TCP and gRPC targets use `host:port`.

The config package derives planned control names with `Route.UnarySubject()`, `Route.OpenSubject()`, and `Route.QueueGroup()`.

## Development

```sh
go test ./...
```
