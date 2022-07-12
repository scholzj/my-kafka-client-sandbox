package cz.scholz.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyAdminClientListAcls {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:32437");

        AdminClient admin = AdminClient.create(props);

        KafkaPrincipal principal = new KafkaPrincipal("User", "my-user");

        // List ACLs
        AclBindingFilter aclBindingFilter = new AclBindingFilter(
                ResourcePatternFilter.ANY,
                new AccessControlEntryFilter(principal.toString(), null, AclOperation.ANY, AclPermissionType.ANY)
        );

        Collection<AclBinding> acls = admin.describeAcls(aclBindingFilter).values().get();
        acls.forEach(a -> {
            System.out.println("-I- ACL");
            System.out.println("      pattern:" + a.pattern());
            System.out.println("      entry:" + a.entry());
        });
    }
}