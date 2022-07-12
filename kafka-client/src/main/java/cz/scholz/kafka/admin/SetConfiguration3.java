package cz.scholz.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class SetConfiguration3 {
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

        AlterConfigsOptions options = new AlterConfigsOptions().validateOnly(false).timeoutMs(null);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource configResource2 = new ConfigResource(ConfigResource.Type.BROKER, "13");

        Set<AlterConfigOp> changes = new HashSet<>();
        //changes.add(new AlterConfigOp(new ConfigEntry("password.encoder.secret", "changeme"), AlterConfigOp.OpType.SET));
        changes.add(new AlterConfigOp(new ConfigEntry("broker.rack", "zone-1a"), AlterConfigOp.OpType.SET));

        Set<AlterConfigOp> changes2 = new HashSet<>();
        changes2.add(new AlterConfigOp(new ConfigEntry("broker.rack", "zone-1c"), AlterConfigOp.OpType.SET));

        Map<ConfigResource,Collection<AlterConfigOp>> configs = new HashMap<>();
        configs.put(configResource, changes);
        configs.put(configResource2, changes2);

        AlterConfigsResult result = admin.incrementalAlterConfigs(configs, options);
        result.all().get();
        Map<ConfigResource, KafkaFuture<Void>> resultMap = result.values();

        for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : resultMap.entrySet())    {
            System.out.println("     " + entry.getKey().toString() + ":" + entry.getValue().get());
        }

    }
}