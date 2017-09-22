# How to start the Zookeeper cluster

```
java -cp zookeeper-3.4.10.jar:lib/slf4j-api-1.6.1.jar:lib/slf4j-log4j12-1.6.1.jar:lib/log4j-1.2.16.jar:conf -Djava.security.auth.login.config=../../my-kafka-client-sandbox/kafka-config/zookeeper/jaas.config -D org.apache.zookeeper.server.quorum.QuorumPeerMain ../../my-kafka-client-sandbox/kafka-config/zookeeper/zookeeper-1.properties
java -cp zookeeper-3.4.10.jar:lib/slf4j-api-1.6.1.jar:lib/slf4j-log4j12-1.6.1.jar:lib/log4j-1.2.16.jar:conf -Djava.security.auth.login.config=../../my-kafka-client-sandbox/kafka-config/zookeeper/jaas.config -D org.apache.zookeeper.server.quorum.QuorumPeerMain ../../my-kafka-client-sandbox/kafka-config/zookeeper/zookeeper-2.properties
java -cp zookeeper-3.4.10.jar:lib/slf4j-api-1.6.1.jar:lib/slf4j-log4j12-1.6.1.jar:lib/log4j-1.2.16.jar:conf -Djava.security.auth.login.config=../../my-kafka-client-sandbox/kafka-config/zookeeper/jaas.config -D org.apache.zookeeper.server.quorum.QuorumPeerMain ../../my-kafka-client-sandbox/kafka-config/zookeeper/zookeeper-3.properties
```