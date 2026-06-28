# airpc

airpc is an outbound-only edge/connector gateway. In this slice it supports HTTP unary proxying over NATS Core request/reply:

- an **edge** process listens for public HTTP requests;
- a private **connector** process dials NATS and queue-subscribes for configured routes;
- each HTTP request is encoded as a versioned MessagePack envelope and sent to `airpc.v1.route.<route>.unary`;
- connector instances share the queue group `airpc.route.<route>.connectors`; and
- the connector forwards the request to the private HTTP target and returns a MessagePack response envelope.

TCP, WebSocket, gRPC tunneling, PKI/mTLS, Kubernetes manifests, and SDKs are not implemented in this slice.

## Run HTTP unary locally

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

## Configuration

See [`examples/airpc.yaml`](examples/airpc.yaml). Important fields for HTTP unary are:

- `nats.url`: NATS server URL used by both edge and connector.
- `edge.http_addr`: HTTP listen address for the edge.
- `routes[].name`: route token used in NATS subjects.
- `routes[].mode: http`: enables HTTP unary runtime for that route.
- `routes[].public_host` (optional): host to match at the edge.
- `routes[].public_prefix` or `routes[].public_path`: public path match.
- `routes[].target`: private backend base URL for the connector.
- `routes[].max_inline_request` / `routes[].max_inline_response`: bounded MessagePack body sizes.
- `routes[].timeout`: request deadline.
- `routes[].forwarded_headers`: allow-list for request headers sent to the connector/backend.

Derived NATS names:

- Unary subject: `airpc.v1.route.<route>.unary`
- Open subject reserved for future streams: `airpc.v1.route.<route>.open`
- Queue group: `airpc.route.<route>.connectors`

The edge strips hop-by-hop headers and only forwards request headers listed in `forwarded_headers`. Response hop-by-hop headers are stripped before writing back to the client. The runtime does not log request/response bodies or Authorization values.

## Development

```sh
go test ./...
```
