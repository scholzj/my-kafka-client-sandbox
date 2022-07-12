package cz.scholz.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyAdminClientAddAcls {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:32437");

        AdminClient admin = AdminClient.create(props);

        KafkaPrincipal principal = new KafkaPrincipal("User", "my-user");

        // Create ACL
        AclBinding acl = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
                new AccessControlEntry(principal.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );

        admin.createAcls(List.of(acl)).all().get();
    }
}