#!/usr/bin/env bash

ZOOKEEPER_NODE="3"

CONFIG_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ! -f /tmp/zookeeper-${ZOOKEEPER_NODE}/myid ]; then
    echo "Creating myid file"
    cp -r ${CONFIG_DIR}/tmp/zookeeper-${ZOOKEEPER_NODE} /tmp/
fi

export SERVER_JVMFLAGS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
    bin/zkServer.sh start-foreground ${CONFIG_DIR}/zookeeper-${ZOOKEEPER_NODE}.properties
