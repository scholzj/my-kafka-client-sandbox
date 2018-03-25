#!/usr/bin/env bash

CONFIG_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

bin/connect-distributed.sh ${CONFIG_DIR}/worker-2.properties
