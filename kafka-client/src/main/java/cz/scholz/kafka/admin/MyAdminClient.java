package cz.scholz.kafka.admin;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.SslConfigs;

public class MyAdminClient {
    private static int timeout = 30000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        //System.setProperty("javax.net.debug", "ssl");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-int:19092");
        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/Users/jakub/development/my-kafka-client-sandbox/ssl-ca/keys/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/jakub/development/my-kafka-client-sandbox/ssl-ca/keys/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"); // Hostname verification

        AdminClient admin = AdminClient.create(props);
        DescribeTopicsResult result = admin.describeTopics(Arrays.asList(new String[]{"test"}));

        Map<String, TopicDescription> topics = result.all().get();

        for (TopicDescription topic : topics.values())
        {
            System.out.println("-I- Topic name " + topic.name());
            System.out.println("    - Internal: " + topic.isInternal());
            System.out.println("    - Partitions: ");

            for (TopicPartitionInfo partition : topic.partitions())
            {
                System.out.println("      - no: " + partition.partition() + "; leader: " + partition.leader() + "; replicas: " + partition.replicas() + "; isr: " + partition.isr());
            }

            System.out.println();
        }

    }
}