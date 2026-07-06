# Guide: Observability

Set `metrics_addr` on either process to get a Prometheus endpoint:

```yaml
edge:
  metrics_addr: 127.0.0.1:9090      # unauthenticated — keep it off public interfaces
connector:
  metrics_addr: 127.0.0.1:9090
```

```sh
curl -s http://127.0.0.1:9090/metrics | grep ^airpc_
```

Privacy invariant: **bodies, header values, and tokens never appear in metrics** — labels are limited to route names, connector IDs, gRPC method paths, and status codes. The same applies to logs (the runtime logs tunnel connect/disconnect events and nothing sensitive).

## Metrics reference

### Edge — HTTP unary

| Metric                                             | Labels             | Meaning                                    |
| -------------------------------------------------- | ------------------ | ------------------------------------------ |
| `airpc_http_requests_total`                        | `route`, `status`  | Requests handled, by response status code  |
| `airpc_http_request_duration_seconds` (histogram)  | `route`            | Headers-to-last-byte duration              |

### Edge — stream sessions (tcp / grpc / websocket)

| Metric                          | Labels                | Meaning                                              |
| ------------------------------- | --------------------- | ---------------------------------------------------- |
| `airpc_stream_sessions_total`   | `route`, `mode`       | Sessions opened                                      |
| `airpc_stream_sessions_active`  | `route`, `mode`       | Sessions currently relaying                          |
| `airpc_stream_bytes_total`      | `route`, `direction`  | Relayed bytes on tcp/grpc; `in` = client→backend     |

### Connector — tunnel health

| Metric                       | Labels                      | Meaning                                    |
| ---------------------------- | --------------------------- | ------------------------------------------ |
| `airpc_tunnel_connected`     | `connector_id`              | 1 while the data tunnel is up              |
| `airpc_tunnel_dials_total`   | `connector_id`, `outcome`   | Dial attempts: `connected` / `failed`      |

### Edge — passive gRPC observation

| Metric                                          | Labels                      | Meaning                                        |
| ----------------------------------------------- | --------------------------- | ---------------------------------------------- |
| `airpc_grpc_rpcs_total`                         | `route`, `method`, `code`   | Completed RPCs by full method and grpc-status  |
| `airpc_grpc_rpc_duration_seconds` (histogram)   | `route`, `method`           | Request headers → trailers                     |

## How gRPC observation works (and when it goes dark)

On `grpc` routes the edge tees the relayed bytes into a bounded queue feeding an HTTP/2 frame + HPACK decoder. It extracts `:path` from request HEADERS and `grpc-status` from trailers (RST_STREAM counts as `canceled`) — message payloads are never inspected.

Guarantees, in order of priority:

1. **Traffic is never touched.** The decoder works on copies; it cannot block, delay, or modify the stream.
2. **Observation is best-effort.** If the queue overflows under load, or the stream isn't parseable plaintext HTTP/2, observation goes dark *for that connection* and the relay continues.

"Goes dark" applies in particular to **TLS passthrough** routes — the edge sees ciphertext. To keep per-method metrics, terminate TLS at the edge (`routes[].tls: true`) or run h2c to the edge. Method label cardinality is bounded: values that don't look like a gRPC method path are recorded as `other`.

## Queries to start with

```promql
# HTTP error rate per route
sum by (route) (rate(airpc_http_requests_total{status=~"5.."}[5m]))
  / sum by (route) (rate(airpc_http_requests_total[5m]))

# HTTP p99 latency
histogram_quantile(0.99, sum by (route, le) (rate(airpc_http_request_duration_seconds_bucket[5m])))

# gRPC error rate per method (grpc-status != 0)
sum by (method) (rate(airpc_grpc_rpcs_total{code!="0"}[5m]))

# Alert: any connector without a live tunnel
airpc_tunnel_connected == 0

# Throughput per tcp/grpc route
sum by (route) (rate(airpc_stream_bytes_total[5m]))
```

Worth alerting on: `airpc_tunnel_connected == 0` for more than a minute, a sustained rise in `airpc_tunnel_dials_total{outcome="failed"}`, HTTP `503` rate (no connectors on a route), and `504` rate (backends slower than route timeouts).
