package cz.scholz.kafka.kafkaclients;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;

public class MessageConsumerSSL {
    private static int timeout = 60000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args) throws TimeoutException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        //System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer.internals.Fetcher", "debug");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        //System.setProperty("kubernetes.impersonate.username", "pepa");
        //System.setProperty("kubernetes.impersonate.group", "pepa");
        //System.setProperty("kubeconfig", "/Users/scholzj/development/aws-kubernetes/.kubeconfig");

        //System.setProperty("javax.net.debug", "ssl");
        Map<String, Object> props = new HashMap();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-35-170-38-11.compute-1.amazonaws.com:30330");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.127.0.0.1.nip.io:443");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.ci-ln-b0kh4xt-d5d6b.origin-ci-int-aws.dev.rhcloud.com:443");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.ci-ln-n2zbjb2-72292.origin-ci-int-gce.dev.rhcloud.com:443");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.192.168.64.156.nip.io:443");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.192.168.64.174.nip.io:443");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.put(ConsumerConfig.CLIENT_RACK_CONFIG, "eu-west-1c");

        //props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "${env:SECURITY_PROTOCOL}");

        //System.setProperty("kubeconfig", "/Users/scholzj/development/aws-kubernetes/.kubeconfig");
        props.put("config.providers", "secrets,env");
        props.put("config.providers.secrets.class", "io.strimzi.kafka.KubernetesSecretConfigProvider");
        props.put("config.providers.env.class", "io.strimzi.kafka.EnvVarConfigProvider");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "${secrets:myproject/my-user:user.crt}");
        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "${secrets:myproject/my-user:user.key}");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "${secrets:myproject/my-cluster-cluster-ca-cert:ca.crt}");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"); // Hostname verification

        /*props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");*/
        /*props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/user.p12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");*/

        /*props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");*/
        //props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Hostname verification

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //consumer.subscribe(Pattern.compile(".*kafka-test-apps"));
        //consumer.subscribe(Pattern.compile("kafka-test-apps"));
        //consumer.subscribe(Collections.singletonList("timer-topic"));
        consumer.subscribe(Collections.singletonList("kafka-test-apps"));
        //consumer.subscribe(Arrays.asList("strimzi-search", "scholzj-timeline"));
        //consumer.subscribe(Arrays.asList("scholzj-timeline"));

        // =====================
        // Recover offsets
        // =====================
        //props.put("replication.policy.separator", "-");

        /*System.out.println("Recovering offsets");
        Set<String> clusters = RemoteClusterUtils.upstreamClusters(props);
        Map<TopicPartition, OffsetAndMetadata> highestOffsets = new HashMap<>();

        for (String cluster : clusters) {
            Map<TopicPartition, OffsetAndMetadata> newOffsets = RemoteClusterUtils.translateOffsets(props, cluster, props.get(ConsumerConfig.GROUP_ID_CONFIG).toString(), Duration.ofMinutes(5));

            for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : newOffsets.entrySet())  {
                TopicPartition partition = offset.getKey();
                OffsetAndMetadata remoteOffset = offset.getValue();

                System.out.println("Recovered offset from cluster " + cluster + " for partition " + partition + " to offset " + offset.getValue().offset());


                if (highestOffsets.containsKey(partition) && highestOffsets.get(partition).offset() < remoteOffset.offset())    {
                    highestOffsets.put(partition, remoteOffset);
                } else {
                    highestOffsets.put(partition, remoteOffset);
                }

            }
        }

        ConsumerRebalanceListener offsetHandler = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    if (highestOffsets.containsKey(partition))  {
                        OffsetAndMetadata local = consumer.committed(partition);
                        OffsetAndMetadata remote = highestOffsets.get(partition);

                        if (local.offset() < remote.offset()) {
                            System.out.println("Seeking to " + remote.offset() + " in " + partition);
                            consumer.seek(partition, remote);
                        } else {
                            System.out.println("Local offset for partition " + partition + " is newer than remote offset. No need to seek.");
                        }

                        highestOffsets.remove(partition);
                    }
                }
            }
        };
        // =====================
        // End of recover offsets
        // =====================

        System.out.println("Offsets recovered");

        //consumer.subscribe(Pattern.compile(".*kafka-test-apps"), offsetHandler);
        consumer.subscribe(Pattern.compile("ocp4.checkpoints.internal"), offsetHandler);*/

        Date totalStartTime = new Date();
        Date blockStartTime = new Date();
        int messageNo = 0;
        int size = 0;
        int sizeBlock = 0;

        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            //Thread.sleep(10000);

            if(records.isEmpty()) {
                System.out.println("-I- No message in topic for " + timeout/1000 + " seconds. Finishing ...");
                break;
            }

            for (ConsumerRecord<String, String> record : records)
            {
                size += record.serializedValueSize() + record.serializedKeySize();
                sizeBlock += record.serializedValueSize() + record.serializedKeySize();
                messageNo++;

                if (debug)
                {
                    System.out.println("-I- received message no. " + messageNo + " (offset " + record.offset() + "): " + record.key() + " / " + record.value() + " (from topic " + record.topic() + ", partition " + record.partition() + ", offset " + record.offset() + ")");
                }

                record.headers().forEach(header -> {
                    System.out.println("-I- Header ... key: " + header.key() + "; value: " + new String(header.value()));
                });

                if (messageNo % timeTick == 0) {
                    Date blockEndTime = new Date();
                    System.out.println("-I- " + messageNo + " messages received: " + ((float) timeTick / (blockEndTime.getTime() - blockStartTime .getTime()) * 1000) + " msg/s, " + ((float) sizeBlock / (blockEndTime.getTime() - blockStartTime .getTime()) * 1000 / 1000) + " kB/s, average size " + ((float)sizeBlock/(float)timeTick));
                    blockStartTime = new Date();
                    sizeBlock = 0;
                }
            }

            consumer.commitSync();
        }

        Date blockEndTime = new Date();
        Date totalEndTime = new Date();

        consumer.close();

        if (messageNo % timeTick != 0) {
            System.out.println("-I- " + messageNo + " messages received: " + ((float)(messageNo % timeTick)/(blockEndTime.getTime()-blockStartTime.getTime()-timeout)*1000) + " msg/s, "	+ ((float) sizeBlock / (blockEndTime.getTime() - blockStartTime.getTime()) * 1000 / 1000) + " kB/s, average size " + ((float)sizeBlock/(float)(messageNo % timeTick)));
        }

        System.out.println("-I- ####################################");
        System.out.println("-I- Total " + messageNo + " messages received: avg " + ((float)messageNo/(totalEndTime.getTime()-totalStartTime.getTime()-timeout)*1000) + " msg/s, " + ((float)size/(totalEndTime.getTime()-totalStartTime.getTime()-timeout)*1000 / 1000) + " kB/s, average size " + ((float)size/(float)messageNo));

        consumer.close();
    }
}