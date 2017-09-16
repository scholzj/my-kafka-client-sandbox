#!/usr/bin/env bash

bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic step0 --replication-factor 3 --partitions 1 --config min.insync.replicas=1 --config segment.bytes=1048576
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic step1 --replication-factor 3 --partitions 1 --config min.insync.replicas=1 --config segment.bytes=1048576
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic step2 --replication-factor 3 --partitions 1 --config min.insync.replicas=1 --config segment.bytes=1048576
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic step3 --replication-factor 3 --partitions 1 --config min.insync.replicas=1 --config segment.bytes=1048576