package cz.scholz.kafka.admin;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class GetConfiguration {
    private static int timeout = 30000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient admin = AdminClient.create(props);
        DescribeClusterResult dcr = admin.describeCluster();

        System.out.println("ClusterId: " + dcr.clusterId().get());

        Node controller = dcr.controller().get();
        System.out.println("Controller: " + controller.id() + " (" + controller.host() + ":" + controller.port() + " on rack " + controller.rack() + ")");

        Collection<Node> nodes = dcr.nodes().get();
        for (Node node : nodes) {
            System.out.println("Node: " + node.id() + " (" + node.host() + ":" + node.port() + " on rack " + node.rack() + ")");
        }

        System.out.println("====================");
        System.out.println("");

        DescribeConfigsOptions options = new DescribeConfigsOptions().includeSynonyms(true).timeoutMs(null);

        List<ConfigResource> resources = new ArrayList<ConfigResource>();
        resources.add(new ConfigResource(ConfigResource.Type.BROKER, "0"));
        resources.add(new ConfigResource(ConfigResource.Type.BROKER, "13"));
        //resources.add(new ConfigResource(ConfigResource.Type.BROKER, "1"));
        //resources.add(new ConfigResource(ConfigResource.Type.TOPIC, "my-topic"));

        DescribeConfigsResult configs = admin.describeConfigs(resources, options);
        Map<ConfigResource, Config> configuration = configs.all().get();
        System.out.println("Configuration:");
        for (Map.Entry<ConfigResource, Config> entry : configuration.entrySet())    {
            System.out.println("     " + entry.getKey().toString() + ":");

            for (ConfigEntry configEntry : entry.getValue().entries())  {
                if (!configEntry.isDefault()) {
                    System.out.println("          " + configEntry.name() + "=" + configEntry.value());
                }
            }
        }
    }
}