package cz.scholz.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyAdminClientDeleteAcls {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:32437");

        AdminClient admin = AdminClient.create(props);

        KafkaPrincipal principal = new KafkaPrincipal("User", "my-user");

        // delete ACL
        AclBindingFilter acl = new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
                new AccessControlEntryFilter(principal.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );

        admin.deleteAcls(List.of(acl)).all().get();
    }
}