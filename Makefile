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
