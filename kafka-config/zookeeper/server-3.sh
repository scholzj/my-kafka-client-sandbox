#!/usr/bin/env bash

CONFIG_DIR="$(dirname ${BASH_SOURCE[0]})"

SERVER_JVMFLAGS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
    bin/zkServer.sh start ${CONFIG_DIR}/zookeeper-3.properties
