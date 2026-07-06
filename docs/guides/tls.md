# Guide: TLS & mTLS

airpc has three TLS surfaces, each configured independently:

1. **Public listeners** — clients → edge.
2. **The data tunnel** — connector → edge, ideally locked with mTLS.
3. **Private hops** — connector → backend (`https://` / `wss://` targets), and NATS (`tls://` URL / `creds_file`).

TLS 1.2 is the minimum everywhere.

## 1. Terminate TLS on the edge

```yaml
edge:
  http_addr: :8443
  data_addr: :8444
  tls:
    cert_file: /certs/edge.crt
    key_file: /certs/edge.key
```

This single certificate serves:

- the HTTP listener (all `http` and `websocket` routes → `https://` / `wss://`),
- the data-tunnel listener (connectors must dial `wss://`),
- every `tcp`/`grpc` route that opts in with `tls: true`.

TCP routes that *don't* opt in remain **passthrough**: the client's own TLS session (pinning, backend mTLS) crosses the relay untouched. Choose per route.

Use a certificate valid for the hostname(s) clients actually dial — with `public_host` virtual-hosting, that usually means a SAN per host or a wildcard.

## 2. Lock the tunnel with a private CA (mTLS)

The tunnel is the trust boundary between your zones, so give it more than the shared token. With a private CA:

```yaml
edge:
  tls:
    cert_file: /certs/edge.crt
    key_file: /certs/edge.key
    client_ca_file: /certs/ca.crt        # require verified connector certs (data listener only)

connector:
  edge_data_url: wss://edge.example.com:8444/_airpc/data
  tunnel_token: still-useful-as-second-factor
  tls:
    ca_file: /certs/ca.crt               # trust the private CA for the edge cert
    cert_file: /certs/connector.crt
    key_file: /certs/connector.key
    # server_name: edge.example.com      # if dialing by IP
```

`client_ca_file` applies **only to the data listener** — public clients are never asked for certificates. A connector without a valid client certificate cannot establish a tunnel at all, and its opens are rejected, so traffic flows only through authenticated connectors.

### Generating a throwaway CA for testing

```sh
# CA
openssl ecparam -genkey -name prime256v1 -out ca.key
openssl req -x509 -new -key ca.key -subj "/CN=airpc-test-ca" -days 365 -out ca.crt

# Edge cert (SAN must cover what connectors and clients dial)
openssl ecparam -genkey -name prime256v1 -out edge.key
openssl req -new -key edge.key -subj "/CN=edge.example.com" |
  openssl x509 -req -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
    -extfile <(printf "subjectAltName=DNS:edge.example.com") -out edge.crt

# Connector client cert
openssl ecparam -genkey -name prime256v1 -out connector.key
openssl req -new -key connector.key -subj "/CN=connector-1" |
  openssl x509 -req -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
    -extfile <(printf "extendedKeyUsage=clientAuth") -out connector.crt
```

(The integration suite does the equivalent in-process — see `internal/integration/tls_test.go`.)

## 3. Secure the private hops

- **Backend targets:** use `https://` (http routes) or `wss://` (websocket routes) targets; the connector verifies them with system roots. TCP/gRPC targets are plain sockets — if the backend speaks TLS, prefer passthrough mode so the *client* terminates it.
- **NATS:** use a `tls://` URL, per-role users via `nats.edge_url` / `nats.connector_url`, and `nats.creds_file` for JWT/NKey auth.

## Decision table

| Requirement                                        | Configuration                                             |
| -------------------------------------------------- | ---------------------------------------------------------- |
| Public HTTPS/WSS for HTTP & WebSocket routes        | `edge.tls`                                                 |
| Public TLS for a gRPC/TCP route                     | `edge.tls` + `routes[].tls: true`                          |
| Client keeps end-to-end TLS / pinning to backend    | tcp/grpc route **without** `tls` (passthrough)             |
| Tunnel encrypted                                    | `edge.tls` + `wss://` `edge_data_url`                      |
| Tunnel with private CA + connector client certs     | add `client_ca_file` + `connector.tls`                     |
| Edge behind an existing TLS-terminating LB          | no `edge.tls`; run plaintext behind the LB (tunnel too, if the LB covers `data_addr`) |
