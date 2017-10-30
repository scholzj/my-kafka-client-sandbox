# How to start Kafka Connect cluster

```
bin/connect-distributed.sh /Users/jakub/development/my-kafka-client-sandbox/kafka-config/kafka-connect/worker-0.properties
bin/connect-distributed.sh /Users/jakub/development/my-kafka-client-sandbox/kafka-config/kafka-connect/worker-1.properties
bin/connect-distributed.sh /Users/jakub/development/my-kafka-client-sandbox/kafka-config/kafka-connect/worker-2.properties
```

## Adding connectors

Connectors can be added through the REST interface
```
curl -X POST -H "Content-Type: application/json" --data '{"name": "my-topic-source", "config": {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector", "tasks.max":"1", "topic":"myTopic", "file": "/tmp/my-topic-source.txt" }}' http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data '{"name": "my-topic-sink", "config": {"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector", "tasks.max":"1", "topics":"myTopic", "file": "/tmp/my-topic-sink.txt" }}' http://localhost:8083/connectors
```
