#!/usr/bin/env bash

ZOOKEEPER_NODE="1"

CONFIG_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ! -f /tmp/zookeeper-1/myid ]; then
    echo "Creating myid file"
    cp -r ${CONFIG_DIR}/tmp/zookeeper-${ZOOKEEPER_NODE} /tmp/
fi

# export SERVER_JVMFLAGS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
#     bin/zkServer.sh start-foreground ${CONFIG_DIR}/zookeeper-${ZOOKEEPER_NODE}.properties

export EXTRA_ARGS="-Djava.security.auth.login.config=${CONFIG_DIR}/jaas.config"; \
    bin/zookeeper-server-start.sh ${CONFIG_DIR}/zookeeper-${ZOOKEEPER_NODE}.properties