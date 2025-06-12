#
# Copyright Manetu Inc. All Rights Reserved.
#
#-------------------------------------------------------------------------------
# Make - compiles the primary artifact
#-------------------------------------------------------------------------------
FROM manetu/unified-builder:v3.0 as builder

COPY Makefile go.* *.go /src/
COPY cmd /src/cmd
COPY driver /src/driver
COPY integration /src/integration
COPY schema /src/schema
COPY utils /src/utils

RUN cd /src && make clean bin

ENV GOPATH=/go

RUN go install github.com/jwilder/dockerize@latest

#-------------------------------------------------------------------------------
# Admin Tools
#-------------------------------------------------------------------------------
FROM manetu/unified-builder:v3.0-runtime as admin-tools

ENV TEMPORAL_SCHEMA_PATH=/etc/temporal/schema
COPY schema $TEMPORAL_SCHEMA_PATH

COPY --from=builder /src/target/temporal-cassandra-tool /usr/local/bin/
COPY --from=builder /src/target/temporal /usr/local/bin/

#-------------------------------------------------------------------------------
# Server
#-------------------------------------------------------------------------------
FROM manetu/unified-builder:v3.0-runtime as server

WORKDIR /etc/temporal

ENV TEMPORAL_HOME=/etc/temporal
ENV TEMPORAL_SCHEMA_PATH=/etc/temporal/schema
EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239

COPY docker/scripts .
COPY schema $TEMPORAL_SCHEMA_PATH

RUN ./install-runtime.sh

USER temporal

COPY --from=builder /src/target/temporal* /usr/local/bin/
COPY --from=builder /go/bin/* /usr/local/bin/
COPY docker/config/dynamicconfig/docker.yaml /etc/temporal/config/dynamicconfig/docker.yaml
COPY docker/config/config_template.yaml /etc/temporal/config/config_template.yaml

ENTRYPOINT ["/etc/temporal/entrypoint.sh"]
