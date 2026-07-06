# Guide: Kubernetes

[`deploy/k8s/`](../../deploy/k8s/) contains plain manifests to start from:

| File             | Contents                                                                     |
| ---------------- | ----------------------------------------------------------------------------- |
| `configmap.yaml` | `airpc-config` — the shared `airpc.yaml`                                       |
| `edge.yaml`      | Edge Deployment + Service (`http` 8080, `data` 8081, `metrics` 9090); readiness on `/_airpc/healthz` |
| `connector.yaml` | Connector Deployment (2 replicas); pod name = connector ID                     |

```sh
# edit deploy/k8s/configmap.yaml (routes, targets, NATS URL) first
kubectl apply -f deploy/k8s/
```

You also need a NATS server reachable by both deployments (the config assumes `nats://nats:4222` — any standard NATS install or chart works, JetStream not required).

## How the pieces map to the cluster

- **Edge** is the only thing that needs public exposure. Put a `LoadBalancer` Service or an Ingress in front of the `http` port. `tcp`/`grpc` route listeners need L4 exposure (a `LoadBalancer` port per route, or your ingress controller's TCP passthrough).
- **The `data` port** must be reachable *by connectors* — via the in-cluster Service when connectors run in the same cluster, or via a public/VPN address when they don't. It does not need to be reachable by anyone else.
- **Connectors scale horizontally.** Replicas of the same Deployment join the route queue groups and share load automatically; each uses its pod name as its connector ID, so IDs stay unique and stable enough. `kubectl scale deploy/airpc-connector --replicas=5` is all it takes.

## The interesting topology: connectors in another network

The manifests colocate everything for simplicity, but the architecture shines when they're apart. Typical layouts:

- **Cluster-per-zone:** edge Deployment in a DMZ cluster, connector Deployment in the private cluster, NATS reachable by both (often in the DMZ or a third zone). The private cluster needs **zero** ingress — its connectors dial out to NATS and to the edge's public `data` address (`wss://edge.example.com:8444/_airpc/data`).
- **On-prem behind NAT:** edge + NATS in the cloud; connectors run on-prem (systemd, Nomad, k8s — anything) and dial out. This is the "expose a service from behind NAT" shape, cloudflared-style, but self-hosted.

For anything that crosses untrusted networks, lock the tunnel with mTLS — see the [TLS guide](tls.md).

## Production checklist

- **Secrets:** move `tunnel_token` and TLS keys out of the ConfigMap into Secrets, mounted or templated into the config file. The manifests ship a `replace-me` placeholder on purpose.
- **Config splitting:** edge and connector can consume different renderings of the config — the edge copy doesn't need private target addresses at all; only `routes[].name/mode` + public matching fields must agree between the two.
- **Probes:** edge readiness is wired to `/_airpc/healthz`. For connectors, watch `airpc_tunnel_connected` (see below) — process liveness alone doesn't prove the tunnel is up.
- **Metrics:** both Deployments expose `:9090` `/metrics`; add scrape annotations or a `ServiceMonitor` to taste ([observability guide](observability.md)).
- **Rolling restarts:** edge restarts drop live stream sessions (clients reconnect; connectors redial within ~250 ms and HTTP resumes immediately). Connector restarts are invisible for new traffic as long as another replica stays up — the queue group and the edge's open-retry handle the rest.
- **NATS permissions:** give edge and connector separate NATS users (`nats.edge_url` / `nats.connector_url`) so a compromised edge can't subscribe to route subjects.

## Sibling system

If the same boundary also needs to move files/objects (S3), deploy [air3](https://github.com/terion-name/air3) alongside — same edge/connector/NATS pattern, same zero-inbound property, purpose-built for object bytes.
