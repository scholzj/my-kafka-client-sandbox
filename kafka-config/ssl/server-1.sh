#!/usr/bin/env bash

CONFIG_DIR="$(dirname ${BASH_SOURCE[0]})"

KAFKA_OPTS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
    bin/kafka-server-start.sh ${CONFIG_DIR}/server-1.properties