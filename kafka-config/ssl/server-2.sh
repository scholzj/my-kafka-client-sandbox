#!/usr/bin/env bash

CONFIG_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export KAFKA_OPTS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
    bin/kafka-server-start.sh ${CONFIG_DIR}/server-2.properties