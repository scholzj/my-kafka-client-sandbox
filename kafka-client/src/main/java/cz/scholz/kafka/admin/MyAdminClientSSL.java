package cz.scholz.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyAdminClientSSL {
    private static int timeout = 30000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        //System.setProperty("javax.net.debug", "ssl");
        Properties props = new Properties();
        props.put("security.protocol", "SSL");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:30931");
        props.put("config.providers", "secrets");
        props.put("config.providers.secrets.class", "io.strimzi.kafka.KubernetesSecretConfigProvider");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "${secrets:myproject/my-user:user.crt}");
        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "${secrets:myproject/my-user:user.key}");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "${secrets:myproject/my-cluster-cluster-ca-cert:ca.crt}");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Hostname verification
        props.put(AdminClientConfig.RETRIES_CONFIG, "1"); // Hostname verification

        AdminClient admin = AdminClient.create(props);

        // Creating topics
        // admin.createTopics(Collections.singleton(new NewTopic("kafka-test-apps4", 3, (short) 3))).all().get();

        /*ListOffsetsResult result = admin.listOffsets(Collections.singletonMap(new TopicPartition("my-topic", 0), OffsetSpec.earliest()));
        System.out.println(result.all().get());

        result = admin.listOffsets(Collections.singletonMap(new TopicPartition("my-topic3", 0), OffsetSpec.latest()));
        System.out.println(result.all().get());*/

//        DescribeTopicsResult result = admin.describeTopics(Arrays.asList(new String[]{"kafka-test-apps"}));
//
//        Map<String, TopicDescription> topics = result.all().get();
//
//        for (TopicDescription topic : topics.values())
//        {
//            System.out.println("-I- Topic name " + topic.name());
//            System.out.println("    - Internal: " + topic.isInternal());
//            System.out.println("    - Partitions: ");
//
//            for (TopicPartitionInfo partition : topic.partitions())
//            {
//                System.out.println("      - no: " + partition.partition() + "; leader: " + partition.leader() + "; replicas: " + partition.replicas() + "; isr: " + partition.isr());
//            }
//
//            System.out.println();
//        }

        System.out.println("-I- Feature metadata:");
        admin.describeFeatures().featureMetadata().get().supportedFeatures().forEach((api, versions) -> {
            System.out.println("-I- Feature " + api);
            System.out.println("    - versions: " + versions.toString());
        });

    }
}