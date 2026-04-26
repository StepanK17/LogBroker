APP_NAME := kafka-clone
MODULE := github.com/StepanK17/LogBroker
GO ?= $(shell command -v go 2>/dev/null || echo /opt/homebrew/bin/go)
GOCACHE ?= $(CURDIR)/.gocache
GO_RUN := GOCACHE=$(GOCACHE) $(GO)
BIN_DIR := bin
PROTO_DIR := api/proto
PROTO_FILE := $(PROTO_DIR)/broker.proto
BROKER_CONFIG ?= broker.example.json

.PHONY: help run-broker run-broker-config build clean fmt test coverage proto

help:
	@printf "Available targets:\n"
	@printf "  help               Show this help message\n"
	@printf "  run-broker         Run broker with default config\n"
	@printf "  run-broker-config  Run broker with broker.example.json\n"
	@printf "  build              Build broker and CLI binaries into ./$(BIN_DIR)\n"
	@printf "  clean              Remove local build artifacts\n"
	@printf "  fmt                Run go fmt\n"
	@printf "  test               Run unit/integration tests\n"
	@printf "  coverage           Generate coverage.out\n"
	@printf "  proto              Regenerate protobuf and gRPC Go files\n"

run-broker:
	$(GO_RUN) run ./cmd/broker

run-broker-config:
	$(GO_RUN) run ./cmd/broker -config $(BROKER_CONFIG)

build: $(BIN_DIR)
	$(GO_RUN) build -o $(BIN_DIR)/$(APP_NAME)-broker ./cmd/broker
	$(GO_RUN) build -o $(BIN_DIR)/$(APP_NAME)-cli ./cmd/cli

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

clean:
	rm -rf $(BIN_DIR) $(GOCACHE) coverage.out

fmt:
	$(GO_RUN) fmt ./...

test:
	$(GO_RUN) test ./...

coverage:
	$(GO_RUN) test -coverprofile=coverage.out ./...
	$(GO_RUN) tool cover -func=coverage.out

proto:
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. \
		--go_opt=module=$(MODULE) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILE)
