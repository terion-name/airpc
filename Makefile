BIN_DIR := bin
GO_PACKAGES := ./...
COMPOSE_FILE := deploy/compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)

.PHONY: fmt test build compose-config compose-up compose-down smoke e2e validate clean

fmt:
	gofmt -w cmd internal

test:
	go test $(GO_PACKAGES)

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/airpc ./cmd/airpc
	go build -o $(BIN_DIR)/airpc-e2e ./cmd/airpc-e2e

compose-config:
	$(COMPOSE) config >/dev/null

compose-up:
	$(COMPOSE) up -d --build

compose-down:
	$(COMPOSE) down --remove-orphans

smoke:
	./deploy/scripts/smoke.sh

e2e: compose-config
	set -e; \
	trap '$(COMPOSE) down --remove-orphans' EXIT; \
	$(COMPOSE) up -d --build; \
	./deploy/scripts/smoke.sh

validate: fmt test build compose-config
	git diff --check

clean:
	rm -rf $(BIN_DIR)
