#!/usr/bin/env bash

ZOOKEEPER_NODE="2"

CONFIG_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ! -f /tmp/zookeeper-${ZOOKEEPER_NODE}/myid ]; then
    echo "Creating myid file"
    cp -r ${CONFIG_DIR}/tmp/zookeeper-${ZOOKEEPER_NODE} /tmp/
fi

export EXTRA_ARGS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
    bin/zookeeper-server-start.sh ${CONFIG_DIR}/zookeeper-${ZOOKEEPER_NODE}.properties
