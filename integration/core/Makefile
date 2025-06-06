# Copyright © Manetu, Inc.  All rights reserved

COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

##### Arguments ######
GOOS        ?= $(shell go env GOOS)
GOARCH      ?= $(shell go env GOARCH)
GOPATH      ?= $(shell go env GOPATH)
# Disable cgo by default.
CGO_ENABLED ?= 0

all: target/core-integration-test

target/core-integration-test:
	@printf $(COLOR) "Build $(@) with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go test -c github.com/manetu/temporal-yugabyte/integration/core -o $@

clean:
	@printf $(COLOR) "Delete old core integration binaries..."
	@rm -rf target
