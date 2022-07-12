package cz.scholz.kafka.kafkaclients;

import io.strimzi.kafka.oauth.client.ClientConfig;
import cz.scholz.kafka.kafkaclients.util.RandomStringGenerator;
import io.strimzi.kafka.oauth.common.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MessageProducerOauthKeycloak {
    private static int count = 100;
    private static int timeTick = 100;
    private static int messageSize = 1024;

    private static Boolean debug = false;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        //System.setProperty("com.sun.jndi.ldap.object.disableEndpointIdentification", "true");
        //System.setProperty("com.sun.net.ssl.checkRevocation", "false");

        /*final String KEYCLOAK_ADDRESS = "http://sso-sso-app-demo.192.168.64.106.nip.io";
        final String REALM = "demo";
        final String TOKEN_ENDPOINT_URI = KEYCLOAK_ADDRESS + "/auth/realms/" + REALM + "/protocol/openid-connect/token";

        System.setProperty(ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, TOKEN_ENDPOINT_URI);
        System.setProperty(Config.OAUTH_CLIENT_ID, "kafka-producer-client");
        System.setProperty(Config.OAUTH_CLIENT_SECRET, "kafka-producer-client-secret");
        //System.setProperty(Config.OAUTH_USERNAME_CLAIM, "preferred_username");*/

        //System.setProperty(ClientConfig.OAUTH_ACCESS_TOKEN, "eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJteXByb2plY3QiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlY3JldC5uYW1lIjoia2Fma2EtcHJvZHVjZXItdG9rZW4tYzlmOGoiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoia2Fma2EtcHJvZHVjZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIxOGZiYzM5Ny1hY2JhLTQ3MDktODYzYi0yOWMwMGRkZjkzM2YiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6bXlwcm9qZWN0OmthZmthLXByb2R1Y2VyIn0.MuJCHdGL5C2Yfyj-1JlavIBkqi0tic0ahzwr3_YMNLMgO65LRffKBhXFDym8taJe2_RHVPDDzEsMRDdMfz3b3KdzulRSZUd6hPBlpl-vzH_RorHiWyb2SAgv8yVZAI_JP4H4bCjY4PQBcBzt1aGFk4HeA9aC0OJ8fr2HSyq-P4mETW6tCyuhyYu3XTXlVv3aUcw-T3mWf13yJHbFf79vy33tP7Bar9k9fKyDlNM8O-TSoBn5Eznzdc4UOk6MxvfORE3WXo7LEjxN9E51enxxEqFS_Pp-fs05uPla55CFaJSYDI0LWewMAHggAXg-mgBSQQ2BSqLywqBfh_8_nWx1gg");

        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.192.168.64.109.nip.io:443");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.jscholz.rhmw-integrations.net:443");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi-kafka-operator/hacking/truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");

        //System.setProperty(Config.OAUTH_SSL_TRUSTSTORE_LOCATION, "/Users/scholzj/development/strimzi-kafka-operator/hacking/oauth/keycloak.truststore");
        //System.setProperty(Config.OAUTH_SSL_TRUSTSTORE_PASSWORD, "123456");

        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required token=eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJteXByb2plY3QiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlY3JldC5uYW1lIjoic3RyaW16aS1jbHVzdGVyLW9wZXJhdG9yLXRva2VuLXE5Z2Q4Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6InN0cmltemktY2x1c3Rlci1vcGVyYXRvciIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjI4YmQxN2U3LWE3ZjctMTFlOS05YzA2LTMyMWYxYWMxZjU5ZCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpteXByb2plY3Q6c3RyaW16aS1jbHVzdGVyLW9wZXJhdG9yIn0.VUut5rtG4tAheJj33LiK5qhd4dFaS7E9bXGbkn2Hf5I2p3qyc-C5ise1fy37mgTo1wW9cP188UQb-rqn7MEbR3rELMbBP42UgI5eE8kybr-MUvyDABElqEXlvuLt_yPLNOlZEhvv2X4b_Y9papj7dx4YKO-MEeHzfKnhdV4R2PUwEtU6RyvfXDkDrTTRlhdEzlVo_5vdemyaEvw4Epfu0yGKH4ZQAgRkTfuB0d08o9HvUcsRLCJxdFM6eaqepN2gfo5DK2rZNgoDBWl-ny5-ZoqKlY4x-5rN52e3Av_VgRGMrLA-SbVq7u6s6n_0g0ILFQREsfdJaJuvWf8mFQ_h9A;");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required token=eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJteXByb2plY3QiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlY3JldC5uYW1lIjoia2Fma2EtcHJvZHVjZXItdG9rZW4tYzlmOGoiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoia2Fma2EtcHJvZHVjZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIxOGZiYzM5Ny1hY2JhLTQ3MDktODYzYi0yOWMwMGRkZjkzM2YiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6bXlwcm9qZWN0OmthZmthLXByb2R1Y2VyIn0.MuJCHdGL5C2Yfyj-1JlavIBkqi0tic0ahzwr3_YMNLMgO65LRffKBhXFDym8taJe2_RHVPDDzEsMRDdMfz3b3KdzulRSZUd6hPBlpl-vzH_RorHiWyb2SAgv8yVZAI_JP4H4bCjY4PQBcBzt1aGFk4HeA9aC0OJ8fr2HSyq-P4mETW6tCyuhyYu3XTXlVv3aUcw-T3mWf13yJHbFf79vy33tP7Bar9k9fKyDlNM8O-TSoBn5Eznzdc4UOk6MxvfORE3WXo7LEjxN9E51enxxEqFS_Pp-fs05uPla55CFaJSYDI0LWewMAHggAXg-mgBSQQ2BSqLywqBfh_8_nWx1gg;");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\";");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"kafka-bridge\" oauth.token.endpoint.uri=\"http://sso-sso-app-demo.192.168.64.106.nip.io/auth/realms/demo/protocol/openid-connect/token \" oauth.client.secret=\"0d594f36-e9ee-4066-b98a-13933667ef40\"  ;");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"ext-kafka-producer\" oauth.token.endpoint.uri=\"https://sso-myproject.apps.jscholz.rhmw-integrations.net/auth/realms/external/protocol/openid-connect/token\" oauth.client.secret=\"ext-kafka-producer-secret\" oauth.ssl.truststore.location=\"/Users/scholzj/development/strimzi-kafka-operator/hacking/oauth/keycloak/keycloak.truststore\" oauth.ssl.truststore.password=\"123456\" oauth.ssl.endpoint.identification.algorithm=\"\";");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        Date totalStartTime = new Date();
        Date blockStartTime = new Date();
        int messageNo = 0;
        int size = 0;
        int sizeBlock = 0;

        while (messageNo < count)
        {
            messageNo++;

            ProducerRecord record = new ProducerRecord<String, String>("kafka-test-apps", "MSG-" + messageNo, RandomStringGenerator.getSaltString(messageSize));
            RecordMetadata result = (RecordMetadata) producer.send(record).get();

            size += result.serializedValueSize() + result.serializedKeySize();
            sizeBlock += result.serializedValueSize() + result.serializedKeySize();

            if (debug)
            {
                System.out.println("-I- sent message no. " + messageNo + " to offset " + result.offset() + ": " + record.key() + " / " + record.value());
            }

            if (messageNo % timeTick == 0) {
                Date blockEndTime = new Date();
                System.out.println("-I- " + messageNo + " messages sent: " + ((float) timeTick / (blockEndTime.getTime() - blockStartTime .getTime()) * 1000) + " msg/s, " + ((float) sizeBlock / (blockEndTime.getTime() - blockStartTime .getTime()) * 1000 / 1000) + " kB/s, average size " + ((float)sizeBlock/(float)timeTick));
                blockStartTime = new Date();
                sizeBlock = 0;
            }
        }

        Date blockEndTime = new Date();
        Date totalEndTime = new Date();

        producer.close();

        if (messageNo % timeTick != 0) {
            System.out.println("-I- " + messageNo + " messages sent: " + ((float)(messageNo % timeTick)/(blockEndTime.getTime()-blockStartTime.getTime())*1000) + " msg/s, "	+ ((float) sizeBlock / (blockEndTime.getTime() - blockStartTime.getTime()) * 1000 / 1000) + " kB/s, average size " + ((float)sizeBlock/(float)(messageNo % timeTick)));
        }

        System.out.println("-I- ####################################");
        System.out.println("-I- Total " + messageNo + " messages sent: avg " + ((float)messageNo/(totalEndTime.getTime()-totalStartTime.getTime())*1000) + " msg/s, " + ((float)size/(totalEndTime.getTime()-totalStartTime.getTime())*1000 / 1000) + " kB/s, average size " + ((float)size/(float)messageNo));

        producer.close();
    }


}