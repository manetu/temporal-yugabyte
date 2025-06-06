# Copyright © Manetu, Inc.  All rights reserved

COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

OUTPUT = target/uberjar/clojure-integration-test
SHELL=/bin/bash -o pipefail

DEPS = Makefile project.clj $(shell find src -type f)

all: bin

bin: $(OUTPUT).jar

$(OUTPUT).jar: $(DEPS)
	@printf $(COLOR) "Build $(@)"
	lein uberjar

clean:
	@printf $(COLOR) "Delete old clojure integration binaries..."
	lein clean
