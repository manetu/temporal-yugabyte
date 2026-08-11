# Copyright © Manetu, Inc.  All rights reserved

export PROJECT_NAME := temporal-yugabyte
export IMAGE_REPOSITORY :=  registry.gitlab.com/manetu/users/$(USER)/$(PROJECT_NAME)

DOCKER_TARGETS = server admin-tools integration-test
DOCKER_TAG ?= latest
SCHEMA_TOOL ?= $(shell which temporal-cassandra-tool)

COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

##### Arguments ######
GOOS        ?= $(shell go env GOOS)
GOARCH      ?= $(shell go env GOARCH)
GOPATH      ?= $(shell go env GOPATH)
# Disable cgo by default.
CGO_ENABLED ?= 0

ALL_SRC         := $(shell find . -name "*.go")
ALL_SRC         += go.mod

# Optional args to create multiple keyspaces:
# make install-schema TEMPORAL_DB=temporal2
TEMPORAL_DB ?= temporal

YB_ENDPOINT=localhost
YB_PORT=9042
ES_ENDPOINT=localhost
ES_PORT=9200

all: bin integration

install-schema-es:
	@printf $(COLOR) "Install Elasticsearch schema..."
	curl --fail -X PUT "http://$(ES_ENDPOINT):$(ES_PORT)/_cluster/settings" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/cluster_settings_v7.json --write-out "\n"
	curl --fail -X PUT "http://$(ES_ENDPOINT):$(ES_PORT)/_template/temporal_visibility_v1_template" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/index_template_v7.json --write-out "\n"
# No --fail here because create index is not idempotent operation.
	curl -X PUT "http://$(ES_ENDPOINT):$(ES_PORT)/temporal_visibility_v1_dev" --write-out "\n"

install-schema-yb:
	@printf $(COLOR) "Install Yugabyte schema..."
	$(SCHEMA_TOOL) -ep $(YB_ENDPOINT) -p $(YB_PORT) drop -k $(TEMPORAL_DB) -f
	$(SCHEMA_TOOL) -ep $(YB_ENDPOINT) -p $(YB_PORT) create -k $(TEMPORAL_DB) --rf 1
	$(SCHEMA_TOOL) -ep $(YB_ENDPOINT) -p $(YB_PORT) -k $(TEMPORAL_DB) setup-schema -v 0.0
	$(SCHEMA_TOOL) -ep $(YB_ENDPOINT) -p $(YB_PORT) -k $(TEMPORAL_DB) update-schema -d ./schema/yugabyte/temporal/versioned

install-schema: install-schema-es install-schema-yb

bin: target/temporal-server target/temporal-cassandra-tool target/temporal

.PHONY: integration
integration:
	cd integration && $(MAKE)

##### Integration testing ######
INTEGRATION_COMPOSE ?= docker/docker-compose/integrate.yml
INTEGRATION_NETWORK ?= integration-test
BUILDER_IMAGE       ?= manetu/unified-builder:v3.2
RUNTIME_IMAGE       ?= manetu/unified-builder:v3.2-jre
DOCKER_RUN          := docker run --rm -v "$(CURDIR):/work" -w /work
BUILD_CACHE_MOUNTS  ?= -v temporal-yb-gocache:/root/.cache/go-build -v temporal-yb-gomod:/go/pkg/mod -v temporal-yb-m2:/root/.m2

.PHONY: integration-build integration-up integration-down integration-run integration-test

integration-build:
	@printf $(COLOR) "Build integration artifacts in $(BUILDER_IMAGE)..."
	$(DOCKER_RUN) $(BUILD_CACHE_MOUNTS) -e GOFLAGS=-buildvcs=false $(BUILDER_IMAGE) make clean all

integration-up:
	@printf $(COLOR) "Start integration dependencies..."
	docker compose -f $(INTEGRATION_COMPOSE) up --quiet-pull --wait -d

integration-down:
	@printf $(COLOR) "Stop integration dependencies..."
	docker compose -f $(INTEGRATION_COMPOSE) down

integration-run:
	@printf $(COLOR) "Run integration suite..."
	$(DOCKER_RUN) -i --network $(INTEGRATION_NETWORK) $(RUNTIME_IMAGE) ./integration/run.sh

# Always builds first to avoid silently re-running stale (or wrong-platform) binaries,
# and always tears down dependencies, even on failure.
integration-test: integration-build integration-up
	@$(MAKE) integration-run; status=$$?; $(MAKE) integration-down; exit $$status

##### Unit testing ######
# Every package except integration/core, which needs a live YugabyteDB. Deriving the
# list means new packages that grow tests are picked up automatically.
UNIT_TEST_PKGS ?= $(shell go list ./... | grep -v '/integration/')
GO_TEST_FLAGS  ?=

.PHONY: unit-test test

unit-test:
	@printf $(COLOR) "Run unit tests..."
	go test $(GO_TEST_FLAGS) $(UNIT_TEST_PKGS)

# Unit tests first: they take seconds and need no infrastructure, so a failure here
# saves the multi-minute build-and-container round trip in integration-test.
test: unit-test integration-test

target/temporal-server: $(ALL_SRC)
	@printf $(COLOR) "Build $(@) with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(EXTRA_SERVER_BUILD_FLAGS) -o $@ ./cmd/server

target/temporal-cassandra-tool: $(ALL_SRC)
	@printf $(COLOR) "Build $(@) with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build -o $@ ./cmd/tools/cassandra

target/temporal: $(ALL_SRC)
	@printf $(COLOR) "Build $(@) with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build -o $@ ./cmd/tools/cli

.PHONY: release
release: $(patsubst %,docker.%,$(DOCKER_TARGETS))

docker.%:
	@printf $(COLOR) "Build docker target: $@"
	docker build -t $(IMAGE_REPOSITORY)/$*:$(DOCKER_TAG) \
	  --no-cache \
	  --target $* \
	  --build-arg GITLAB_TOKEN_READ_REPOSITORY="$(GITLAB_TOKEN_READ_REPOSITORY)" \
	  --build-arg GOPRIVATE="$(GOPRIVATE)" \
	  $(DOCKER_BUILD_ARGS) \
	  .
	docker push $(IMAGE_REPOSITORY)/$*:$(DOCKER_TAG)

clean:
	@printf $(COLOR) "Delete old server binaries..."
	cd integration && $(MAKE) clean
	@rm -rf target .artifacts
