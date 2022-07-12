package cz.scholz.kafka.kafkaclients;

import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.common.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class MessageConsumerOauthKeycloak {
    private static int timeout = 5000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args)
    {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        /*System.setProperty(ClientConfig.OAUTH_SSL_TRUSTSTORE_CERTIFICATES, "-----BEGIN CERTIFICATE-----\n" +
                "MIIDDDCCAfSgAwIBAgIBATANBgkqhkiG9w0BAQsFADAmMSQwIgYDVQQDDBtpbmdy\n" +
                "ZXNzLW9wZXJhdG9yQDE2MjMyNjgzOTQwHhcNMjEwNjA5MTk1MzE0WhcNMjMwNjA5\n" +
                "MTk1MzE1WjAmMSQwIgYDVQQDDBtpbmdyZXNzLW9wZXJhdG9yQDE2MjMyNjgzOTQw\n" +
                "ggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC0sMv9XFJXYExcjx39tHyG\n" +
                "ckiprgD58zFHH+Del0IV4HRdsZvC9y3AMvI742sWg8IKfis4//DkUSXC731fgRYi\n" +
                "L/uMbd+VtmeRCgTaFW6PS9w+LUdgzg4HO6x6i/csL/8u0TMgOy1gv06Ysp0OZIo0\n" +
                "dOVSw9YHyu5oozCw7sUeFsiJ8/DVM6qk/h9mBelQwA0T/S80e0yFpYqAV5x+/QFe\n" +
                "nLbBM6E3XOaaqLRAzaXqjDsWlA58amoEeQsB2KYVGrQTtCYkbM2Hteuq68L+af7p\n" +
                "453vIgkEnTgyGFBGYxbWjT4nZYVlC6dliRH4KERdHqTnH9cnJOA/sKLcgI48LDKN\n" +
                "AgMBAAGjRTBDMA4GA1UdDwEB/wQEAwICpDASBgNVHRMBAf8ECDAGAQH/AgEAMB0G\n" +
                "A1UdDgQWBBSBpaU8pP/FH4SSYRgq6jqCOhUX0zANBgkqhkiG9w0BAQsFAAOCAQEA\n" +
                "N10AixZwewMT/CHqdGag1XaQf6q96/eU2ftPLmqi1jlIveS4gT9Gu271NC943kPo\n" +
                "50bMIGWs78Ng3yxfUEMihi/hWIvuEbh1hu3WiNihTrq9XyzrQEZrdhGNEYfb7Xjo\n" +
                "ARdNbMVWHR/uO1loz6lCyncds3QcQjhq1Qn2qpC9hboy/TLoBc/c8bX5lo7IXVVD\n" +
                "iZJ/kPC37Fp7iStuGTzGc4iizZC2VLYY+olMQFf4d3is1kRJKZqH656B0WK70T6f\n" +
                "I+a/ToHPUqgIb8OTiO98SP7u2l3sIby3758sG11sbN3PgqMC8L1+vUaeYS7mZwOK\n" +
                "Y4n3sz2MygojPeNLgCtSRQ==\n" +
                "-----END CERTIFICATE-----\n");
        System.setProperty(ClientConfig.OAUTH_SSL_TRUSTSTORE_TYPE, "PEM");*/

        System.setProperty(ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "https://keycloak-myproject.apps.ci-ln-zltphnk-f76d1.origin-ci-int-gce.dev.openshift.com/auth/realms/External/protocol/openid-connect/token");
        System.setProperty(ClientConfig.OAUTH_CLIENT_ID, "ext-kafka-consumer");
        System.setProperty(ClientConfig.OAUTH_CLIENT_SECRET, "ext-kafka-consumer-secret");
        //System.setProperty(Config.OAUTH_USERNAME_CLAIM, "preferred_username");
        //System.setProperty(Config.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");

        //System.setProperty(Config.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        System.setProperty(Config.OAUTH_SSL_TRUSTSTORE_LOCATION, "/Users/scholzj/development/strimzi/hacking/oauth/keycloak/keycloak.truststore");
        System.setProperty(Config.OAUTH_SSL_TRUSTSTORE_PASSWORD, "123456");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.ci-ln-zltphnk-f76d1.origin-ci-int-gce.dev.openshift.com:443");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.jscholz.rhmw-integrations.net:31313");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));

        Date totalStartTime = new Date();
        Date blockStartTime = new Date();
        int messageNo = 0;
        int size = 0;
        int sizeBlock = 0;

        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            if(records.isEmpty()) {
                System.out.println("-I- No message in topic for " + timeout/1000 + " seconds. Finishing ...");
                continue;
            }

            for (ConsumerRecord<String, String> record : records)
            {
                size += record.serializedValueSize() + record.serializedKeySize();
                sizeBlock += record.serializedValueSize() + record.serializedKeySize();
                messageNo++;

                if (debug)
                {
                    System.out.println("-I- received message no. " + messageNo + ": " + record.key() + " / " + record.value());
                }

                if (messageNo % timeTick == 0) {
                    Date blockEndTime = new Date();
                    System.out.println("-I- " + messageNo + " messages received: " + ((float) timeTick / (blockEndTime.getTime() - blockStartTime .getTime()) * 1000) + " msg/s, " + ((float) sizeBlock / (blockEndTime.getTime() - blockStartTime .getTime()) * 1000 / 1000) + " kB/s, average size " + ((float)sizeBlock/(float)timeTick));
                    blockStartTime = new Date();
                    sizeBlock = 0;
                }
            }
        }
    }
}