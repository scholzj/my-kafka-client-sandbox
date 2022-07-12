package cz.scholz.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyAdminClient {
    private static int timeout = 30000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        AdminClient admin = AdminClient.create(props);
        DescribeTopicsResult result = admin.describeTopics(Arrays.asList(new String[]{"myTopic"}));

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