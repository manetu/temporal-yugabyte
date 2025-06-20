#!/bin/bash

set -eux

export TEMPORAL_CONFIG_ENV=${TEMPORAL_CONFIG_ENV:-integration}
export BINDIR=./target

export CASSANDRA_SEEDS=${CASSANDRA_SEEDS:-yugabyte}
export CASSANDRA_KEYSPACE=temporal

export ES_SEEDS=${ES_SEEDS:-elasticsearch}
export ES_SERVER="http://$ES_SEEDS:9200"
export ES_VERSION=v7
export ES_VIS_INDEX=temporal_visibility_v1_dev

init_yb() {
    until $BINDIR/temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" validate-health; do
        echo 'Waiting for Yugabyte to start up.'
        sleep 1
    done
    echo 'Yugabyte started.'

    SCHEMA_DIR=./schema/yugabyte/temporal/versioned
    $BINDIR/temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" create -k "${CASSANDRA_KEYSPACE}" --rf "1"
    $BINDIR/temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" -k "${CASSANDRA_KEYSPACE}" setup-schema -v 0.0
    $BINDIR/temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" -k "${CASSANDRA_KEYSPACE}" update-schema -d "${SCHEMA_DIR}"
}

init_es() {
    until curl --silent --fail "${ES_SERVER}" >& /dev/null; do
        echo 'Waiting for Elasticsearch to start up.'
        sleep 1
    done
    echo 'Elasticsearch started.'

    SETTINGS_URL="${ES_SERVER}/_cluster/settings"
    SETTINGS_FILE=./schema/elasticsearch/visibility/cluster_settings_${ES_VERSION}.json
    TEMPLATE_URL="${ES_SERVER}/_template/temporal_visibility_v1_template"
    SCHEMA_FILE=./schema/elasticsearch/visibility/index_template_${ES_VERSION}.json
    INDEX_URL="${ES_SERVER}/${ES_VIS_INDEX}"
    curl --fail  -X PUT "${SETTINGS_URL}" -H "Content-Type: application/json" --data-binary "@${SETTINGS_FILE}" --write-out "\n"
    curl --fail -X PUT "${TEMPLATE_URL}" -H 'Content-Type: application/json' --data-binary "@${SCHEMA_FILE}" --write-out "\n"
    curl -X PUT "${INDEX_URL}" --write-out "\n"

}

init_services() {
    init_yb
    init_es
}

start_temporal() {
    $BINDIR/temporal-server --env $TEMPORAL_CONFIG_ENV --allow-no-auth start &
    export PID=$!
    echo "Temporal started on PID: $PID"

    function cleanup() {
        echo "Shutting down Temporal on PID: $PID"
        kill -9 $PID
    }
    trap cleanup EXIT

    until $BINDIR/temporal operator cluster health | grep -q SERVING; do
        echo "Waiting for Temporal server to start..."
        sleep 1
    done
    echo "Temporal service ready."
}

# Initialize system
init_services
start_temporal

# Create our default namespace
$BINDIR/temporal operator namespace create default

# Run the integration suite
integration/core/target/core-integration-test -test.v
java -jar integration/clojure/target/uberjar/clojure-integration-test.jar
