# How to start Kafka cluster

```
export KAFKA_OPTS="-Djava.security.auth.login.config=../my-kafka-client-sandbox/kafka-config/ssl/jaas.config"; bin/kafka-server-start.sh /Users/jakub/development/my-kafka-client-sandbox/kafka-config/ssl/server-0.properties
export KAFKA_OPTS="-Djava.security.auth.login.config=../my-kafka-client-sandbox/kafka-config/ssl/jaas.config"; bin/kafka-server-start.sh /Users/jakub/development/my-kafka-client-sandbox/kafka-config/ssl/server-1.properties
export KAFKA_OPTS="-Djava.security.auth.login.config=../my-kafka-client-sandbox/kafka-config/ssl/jaas.config"; bin/kafka-server-start.sh /Users/jakub/development/my-kafka-client-sandbox/kafka-config/ssl/server-2.properties
```