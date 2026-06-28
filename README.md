# airpc

airpc is an outbound-only RPC tunnel scaffold. The intended runtime will use NATS Core as the control plane and binary, closed-schema MessagePack envelopes for protocol messages. There are no JSON wire protocols.

## Current status

This repository currently contains the compile-able foundations only:

- strict YAML config loading and validation
- airpc subject builders and validators
- HTTP header filtering for safe forwarding
- MessagePack envelope encoding/decoding
- a small NATS Core wrapper with typed error mapping
- a thin `airpc` CLI placeholder

The edge/connector runtime, tunnel relay, and request handling loops are not wired yet.

## Quickstart shape

```sh
go test ./...
go run ./cmd/airpc -config examples/airpc.yaml validate
```

`validate` loads and validates the config. Runtime commands currently return a clear not-yet-wired error until the edge and connector implementations land.

## Intended command shape

```sh
airpc -config /path/to/airpc.yaml validate
airpc -config /path/to/airpc.yaml edge       # not implemented yet
airpc -config /path/to/airpc.yaml connector  # not implemented yet
```

See [`examples/airpc.yaml`](examples/airpc.yaml) for the current config shape.
