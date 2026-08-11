# Testing Guide

This guide covers running the test suites locally. The authoritative definition of what CI runs is [.github/workflows/test.yml](../.github/workflows/test.yml); the commands below reproduce it.

For local development setup, running the server in a debugger, and profiling, see the [Development Guide](./development.md).

## Test Tiers

The repository has three tiers of tests.

**Unit tests** live alongside the driver sources in `driver/` and `utils/`. They require no infrastructure. CI runs them in the `build` job via `make unit-test`.

**The core persistence suite** (`integration/core`) exercises the YCQL driver directly against a live database. It requires YugabyteDB only - no Elasticsearch and no Temporal server. Each suite creates and drops its own randomly named keyspace, so it is safe to run alongside a live Temporal server.

**The Clojure workflow suite** (`integration/clojure`) drives real workflows through a running Temporal server, which in turn requires YugabyteDB and Elasticsearch.

`integration/run.sh` is the CI entry point for the latter two tiers. It provisions the schema, starts a Temporal server, and runs the core suite followed by the Clojure suite.

| Tier | Target |
| --- | --- |
| Unit tests | `make unit-test` |
| Core + Clojure integration suites | `make integration-test` |
| Everything | `make test` |

## Prerequisites

`make unit-test` requires only a Go toolchain (Go 1.26 or later) - no Docker, no other services.

All other modes require [Docker](https://www.docker.com/) with the Compose v2 plugin, roughly 4 GB of free memory, and 1 GB of free disk for build output.

Running the full suite on the host additionally requires Go 1.26 or later, a JRE, and [Leiningen](https://leiningen.org/).

The backing services publish ports 5433, 7000, 9000, 9042, 9200, and 12000. A host-native Temporal server additionally binds 6933-6939, 7233-7243, 7936, and 8000. All must be free.

> Only one of `integrate.yml`, `develop.yml`, and `quick-start.yml` may be running at a time. They publish the same host ports, and `integrate.yml` and `quick-start.yml` also share container names.

`make integration-test` takes a few minutes end to end once the Go build/module cache and Maven repository are warm; expect longer on the very first run while those populate.

## Running the Full Suite in Containers

This is the recommended way to run the suite. It builds and runs in the same images CI uses, so a pass here is meaningful, and it sidesteps host toolchain and networking differences entirely (see "Running the Full Suite on the Host" below).

```shell
make integration-test
```

This tears down the backing services when it finishes, even if the suite fails. It expands to the four steps below, which you can also run individually.

### Step 1: Build

```shell
make integration-build
```

Which runs:

```shell
docker run --rm -v "$PWD:/work" -w /work -e GOFLAGS=-buildvcs=false \
  manetu/unified-builder:v3.2 make clean all
```

> `make clean` is required whenever you switch between building on the host and building in the container. The build targets are timestamp-based, and `integration/core/target/core-integration-test` used to have no prerequisites at all, so `make` can otherwise reuse binaries built for the wrong platform without telling you. See "Exec format error" under Troubleshooting.

> The first build is slow because the Go build cache, module cache, and Maven repository all start empty. `make integration-build` mounts named Docker volumes for these (`temporal-yb-gocache`, `temporal-yb-gomod`, `temporal-yb-m2`) so subsequent builds are incremental.

### Step 2: Start the Backing Services

```shell
make integration-up
```

Which runs `docker compose -f docker/docker-compose/integrate.yml up --quiet-pull --wait -d`, bringing up YugabyteDB and Elasticsearch on a bridge network named `integration-test`.

> Neither service declares a healthcheck, so `--wait` only confirms the containers are running, not that the database or search index is ready to accept connections. Actual readiness is handled by the poll loops inside `integration/run.sh`.

### Step 3: Run the Suite

```shell
make integration-run
```

Which runs:

```shell
docker run --rm -i --network integration-test -v "$PWD:/work" -w /work \
  manetu/unified-builder:v3.2-jre ./integration/run.sh
```

`integration/run.sh` waits for YugabyteDB and Elasticsearch to become reachable, installs the schema, starts a Temporal server, creates the `default` namespace, and then runs the core suite followed by the Clojure suite.

### Step 4: Tear Down

```shell
make integration-down
```

Neither service declares a volume, so this discards all state. Always tear down before re-running - see "Re-running fails at namespace creation" under Troubleshooting.

## Running the Full Suite on the Host

Useful when you want to attach a debugger or profiler to the server, or iterate without a container round trip. This diverges from what CI runs, so treat a pass here as informative, not authoritative.

```shell
make clean
make all
docker compose -f docker/docker-compose/integrate.yml up --quiet-pull --wait
TEMPORAL_CONFIG_ENV=development CASSANDRA_SEEDS=127.0.0.1 ES_SEEDS=127.0.0.1 \
  ./integration/run.sh
```

Run this from the repository root - `integration/run.sh` resolves `./target`, `./config`, and `./schema` relative to the current directory.

`TEMPORAL_CONFIG_ENV=development` selects `config/development.yaml`, which points at `127.0.0.1`. Without it, `run.sh` defaults to `--env integration`, which selects `config/integration.yaml` and the Docker service names `yugabyte`/`elasticsearch` - unreachable by that name from the host.

Tear down the same way:

```shell
docker compose -f docker/docker-compose/integrate.yml down
```

> If the core suite reports `gocql: no hosts available in the connection pool` even though the schema tool connected successfully, your Docker runtime is not routing container IPs back to the host (this varies between OrbStack and Docker Desktop on macOS). Use the containerized mode instead.

## Running a Subset

### Unit Tests

No infrastructure required:

```shell
make unit-test
```

This runs `go test` against every package except `integration/core`, which needs a live YugabyteDB. Do not use `go test ./...` directly - it sweeps in `integration/core` and will fail without one; `make unit-test` derives its package list precisely to avoid that.

To run a single package:

```shell
go test ./driver/...
```

> The Go tests under `benchmarking/ansible/roles/temporal/files/chart/tests` are a separate Go module (their own `go.mod`) that exercises the Helm chart via terratest and `helm`. `make unit-test` does not cover them, and nothing in this repo currently runs them.

### Core Persistence Suite Only

This suite needs YugabyteDB and nothing else. Start just that service, then run the compiled test binary directly:

```shell
docker compose -f docker/docker-compose/integrate.yml up --quiet-pull --wait -d yugabyte
CASSANDRA_SEEDS=127.0.0.1 ./integration/core/target/core-integration-test -test.v
```

Or, matching the containerized mode:

```shell
docker run --rm -i --network integration-test -v "$PWD:/work" -w /work \
  -e CASSANDRA_SEEDS=yugabyte manetu/unified-builder:v3.2-jre \
  ./integration/core/target/core-integration-test -test.v
```

To run a single suite:

```shell
CASSANDRA_SEEDS=127.0.0.1 ./integration/core/target/core-integration-test \
  -test.v -test.run TestYugabyteShardStoreSuite
```

The suite entry points are named `TestYugabyte*`; list them with:

```shell
grep '^func TestYugabyte' integration/core/integration_test.go
```

> `CASSANDRA_PORT` is read but has no effect - it never reaches the underlying driver config, so the suite always connects on 9042.

### Clojure Workflow Suite Only

This suite needs a running Temporal server. Bring one up with the [Development Guide](./development.md), then run it from source:

```shell
cd integration/clojure
lein run
```

Or run the built uberjar from the repository root:

```shell
java -jar integration/clojure/target/uberjar/clojure-integration-test.jar
```

Point either at a non-default server with `TEMPORAL_ADDRESS`:

```shell
TEMPORAL_ADDRESS=localhost:7233 lein run
```

> `lein test` runs nothing. This project has no `test/` directory; the suite is a Greenlight runner invoked through `-main`, so use `lein run` instead.

## Running the CI Workflow with `act`

You can also reproduce the CI *runner* itself using [`act`](https://github.com/nektos/act) - useful for debugging the workflow YAML, but slower and more fragile than the modes above. Prefer `make integration-test` unless you specifically need to exercise the GitHub Actions definition.

### Prerequisites

- [Docker](https://www.docker.com/) installed and running
- [act](https://github.com/nektos/act) - Run GitHub Actions locally

### Running Tests

```shell
act -j test --artifact-server-path $PWD/.artifacts
```

### Required Flags

This project includes an `.actrc` file that configures `act` to use the `--bind` flag. You must also specify `--artifact-server-path` on the command line for artifact handling to work correctly.

**Why is `--artifact-server-path` needed?**

The CI workflow uses GitHub Actions artifacts to pass build outputs between jobs. The `--artifact-server-path` flag tells `act` where to store these artifacts locally, enabling the download-artifact step to retrieve files uploaded by previous jobs.

**Why is `--bind` needed?**

The CI workflow uses docker-in-docker to run integration tests:

```yaml
- name: Run integration tests
  run: docker run -i --network integration-test -v ${{ github.workspace }}:/work ...
```

By default, `act` **copies** the working directory into the container rather than binding it. This causes a problem:

1. Artifacts extracted inside the CI container exist only there
2. When `docker run -v ${{ github.workspace }}:/work` executes, it tries to mount the **host** path
3. The host path doesn't have the extracted artifacts - they only exist inside the first container

The `--bind` flag tells `act` to bind-mount the working directory instead of copying it. This means files created inside the container are written directly to the host filesystem, making them visible to nested `docker run` commands.

### Cleanup

When using `--bind`, files created inside the container persist on the host filesystem. This includes the `target/` directory with compiled binaries. Clean these up with:

```shell
make clean
```

Or manually:

```shell
rm -rf target integration/*/target
```

## Troubleshooting

**Re-running fails at namespace creation.**
`integration/run.sh` calls `temporal operator namespace create default` unconditionally, and that command fails if the namespace already exists. Because the script runs under `set -e`, the run aborts there before either test suite executes. Every earlier step in the script is idempotent - only this one blocks re-runs. Tear the stack down between runs:

```shell
make integration-down
```

As a faster alternative, you can drop just the keyspace while no server is running:

```shell
./target/temporal-cassandra-tool --ep 127.0.0.1 drop -k temporal -f
```

This leaves stale documents behind in the Elasticsearch visibility index, so prefer the full teardown when in doubt.

**`Exec format error`** (may appear as `cannot execute binary file: Exec format error` or, on some Docker runtimes, `Cannot run macOS (Mach-O) executable in Docker: Exec format error`).
You built on the host and then ran in a container, or the reverse. The build targets are timestamp-based and can silently reuse binaries built for the wrong platform. Run `make clean` and rebuild in the mode you intend to run.

**`make install-schema` fails with `-ep: command not found` and `Error 127`.**
The `SCHEMA_TOOL` variable defaults to `$(shell which temporal-cassandra-tool)`, and `make bin` does not install the tool onto `PATH` - it only builds it to `./target/temporal-cassandra-tool`. Point `SCHEMA_TOOL` at the built copy:

```shell
make install-schema SCHEMA_TOOL=./target/temporal-cassandra-tool
```

Unlike `integration/run.sh`, `make install-schema` drops and recreates the keyspace, so it is safe to re-run.

**`port is already allocated` or `container name is already in use`.**
Another compose stack is already up. Only one of `integrate.yml`, `develop.yml`, and `quick-start.yml` can run at a time. Find it with `docker compose ls` and bring it down before starting another.

**Build output is owned by `root`.**
On Linux, the containerized build writes to the bind-mounted directory as `root`, since the container runs as root. Either build on the host instead, or clean up with `sudo rm -rf target integration/*/target`.
