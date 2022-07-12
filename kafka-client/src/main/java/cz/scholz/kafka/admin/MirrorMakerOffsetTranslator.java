package cz.scholz.kafka.admin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

public class MirrorMakerOffsetTranslator {
    private static int timeout = 60000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args) throws TimeoutException, InterruptedException {
        //System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        //System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer.internals.Fetcher", "debug");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        //System.setProperty("javax.net.debug", "ssl");

        Map<String, Object> props = new HashMap();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.192.168.64.131.nip.io:443");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.jscholz.rhmw-integrations.net:443");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-apps");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_RACK_CONFIG, "eu-west-1c");

        props.put("security.protocol", "SSL");

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi-kafka-operator/hacking/truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi-kafka-operator/hacking/user.p12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        /*props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");*/
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"); // Hostname verification

        //props.put("replication.policy.separator", "-");

        System.out.println("Checkpoint topics");
        System.out.println(RemoteClusterUtils.checkpointTopics(props));

        System.out.println("Upstream clusters");
        System.out.println(RemoteClusterUtils.upstreamClusters(props));

        System.out.println("Recovering offsets");
        Map<TopicPartition, OffsetAndMetadata> newOffsets = RemoteClusterUtils.translateOffsets(props, "ocp4", "my-group", Duration.ofMinutes(5));

        for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : newOffsets.entrySet())  {
            System.out.println("Reseting partition " + offset.getKey() + " to offset " + offset.getValue().offset());
            //consumer.seek(offset.getKey(), offset.getValue().offset());
        }

        System.out.println("Offsets recovered");
    }
}