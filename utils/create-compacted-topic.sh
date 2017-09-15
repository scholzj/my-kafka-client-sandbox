#!/usr/bin/env bash

bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic myCompactedTopic --replication-factor 3 --partitions 3 --config min.insync.replicas=1 --config cleanup.policy=compact --config segment.bytes=1048576