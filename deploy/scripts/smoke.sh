#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE=${COMPOSE:-docker compose}
COMPOSE_FILE=${COMPOSE_FILE:-"$ROOT_DIR/deploy/compose.yaml"}

run_compose() {
  # shellcheck disable=SC2086
  $COMPOSE -f "$COMPOSE_FILE" "$@"
}

runner() {
  run_compose run --rm --no-deps -T test-runner airpc-e2e "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}

require_cmd docker

runner wait-health --base-url http://edge:8080 --attempts 60
for i in $(seq 1 30); do
  if runner check --base-url http://edge:8080 --tcp-addr edge:7000 --ws-url ws://edge:8080/ws --grpc-addr edge:7003; then
    break
  fi
  if [ "$i" = "30" ]; then
    echo "error: route checks did not pass" >&2
    exit 1
  fi
  sleep 1
done
runner check-unreachable http-backend:9000 tcp-backend:9001 websocket-backend:9002 grpc-backend:9003
run_compose exec -T edge airpc-e2e check-unreachable http-backend:9000 tcp-backend:9001 websocket-backend:9002 grpc-backend:9003

run_compose stop connector
runner check-down --base-url http://edge:8080 --tcp-addr edge:7000 --ws-url ws://edge:8080/ws --grpc-addr edge:7003
run_compose start connector
runner wait-health --base-url http://edge:8080 --attempts 60
for i in $(seq 1 30); do
  if runner check --base-url http://edge:8080 --tcp-addr edge:7000 --ws-url ws://edge:8080/ws --grpc-addr edge:7003; then
    break
  fi
  if [ "$i" = "30" ]; then
    echo "error: route checks did not recover" >&2
    exit 1
  fi
  sleep 1
done

echo "ok: airpc compose smoke passed"
