# Development Guide

This guide covers local development setup and running CI locally.

## Local Development Setup

### Step 1: Start Backend Dependencies

In one terminal, run the backend dependencies with docker-compose:

```shell
docker-compose -f docker/docker-compose/develop.yml up
```

### Step 2: Install Schema

In a second terminal, run:

```shell
make install-schema
```

### Step 3: Run the Server

From an IDE such as Goland, set up an execution for `./cmd/server` with the following switches:

```shell
--env development --allow-no-auth start
```

Then launch the environment in the debugger.

### Step 4: Create Default Namespace

Once Temporal is running, execute the following command:

```shell
temporal operator namespace create default
```

### Step 5: Use Your Cluster

Helpful suggestions:

1. You can open the Temporal UI by visiting http://localhost:8080
2. You can generate some workflow activity by running `lein test` from the integration directory

## Running CI Locally with `act`

You can run the GitHub Actions CI workflow locally using [`act`](https://github.com/nektos/act).

### Prerequisites

- [Docker](https://www.docker.com/) installed and running
- [act](https://github.com/nektos/act) - Run GitHub Actions locally

### Running Tests

To run the test workflow locally:

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

## Profiling

The development environment integrates with [pprof](https://pkg.go.dev/net/http/pprof). It exposes an HTTP listener on port 7936.

### Tracing

Run the following to generate a 15-second trace:

```shell
curl -o trace.out http://localhost:7936/debug/pprof/trace?seconds=15
```

Once the system completes the trace, you may render it with:

```shell
go tool trace trace.out
```

> Tip: Enable "Flow Events" in the process trace window
