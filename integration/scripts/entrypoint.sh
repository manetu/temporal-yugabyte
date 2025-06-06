#!/bin/bash

set -ex

core-integration-test -test.v
java $JVM_OPTS -jar /usr/local/clojure-integration-test.jar