package cz.scholz.kafka.kafkaclients;

import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.common.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class MessageConsumerOauthAuth0 {
    private static int timeout = 5000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args)
    {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        System.setProperty(ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "https://scholzj.eu.auth0.com/oauth/token");
        //System.setProperty(ClientConfig.OAUTH_ACCESS_TOKEN, "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik9FSTBNa0l6UmpjelEwSkNSalJGUmpjNVJrRkdPVVJFUlRrME0wRkVSVUkwTXpNd09VUkROdyJ9.eyJpc3MiOiJodHRwczovL3NjaG9semouZXUuYXV0aDAuY29tLyIsInN1YiI6ImZUaVdxMkduVWp2ZmQ2aWMzRzFNNkhBcTJzQ1JCcnVpQGNsaWVudHMiLCJhdWQiOiI0ZTFYZnd4MEhIZmgyalRIYm45aVd2dlRvenV5anRhTCIsImlhdCI6MTYxNTg1MDY3NiwiZXhwIjoxNjE1OTM3MDc2LCJhenAiOiJmVGlXcTJHblVqdmZkNmljM0cxTTZIQXEyc0NSQnJ1aSIsImd0eSI6ImNsaWVudC1jcmVkZW50aWFscyJ9.sKrdfMipNnDHCxNGG7XnXOYZPFu1yijWiIOj94RbvkFXcb0htCpBPGjlShxUApYl_j_bKAIeml5B_2IBwXullWCcsUdY0X2vHai1uwqsy2IxesllFeDQmFlYyW5Od36qJVJQrXwjJW1Bz3Ujw5tmGHqcAMfAR_0kOGqcfelVWwMWTQKWZlthD0KHgDcGb_fN0bvvDny0jxUkNtlcka1t8ye5sU4q4BCLpFT21xXLhc2qlXqfX_hr3p84AFM3SDQquFM_9xlOVE5LH3FTvcNLqG7Diw0Ln2ao9l6Bqa5-mvB8UQZwnfsX48meh3Kh5WwTAIMZbWS0XkNrEc0-N6w_ng");
        System.setProperty(Config.OAUTH_CLIENT_ID, "Cjr89bHcrTK15fgmnHLV3ZlMJPEgbOTf");
        System.setProperty(Config.OAUTH_CLIENT_SECRET, "xBwUvcjfNEM9VetlrMKiRO5ZT9oXgd4l8bVPtRDoyJpD6nXoHL_rh2PsJexIXQnm");
        System.setProperty(ClientConfig.OAUTH_AUDIENCE, "my-cluster");
        //System.setProperty(Config.OAUTH_USERNAME_CLAIM, "preferred_username");
        //System.setProperty(Config.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");

        //System.setProperty(Config.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        //System.setProperty(Config.OAUTH_SSL_TRUSTSTORE_LOCATION, "/Users/scholzj/development/strimzi/hacking/oauth/keycloak/keycloak.truststore");
        //System.setProperty(Config.OAUTH_SSL_TRUSTSTORE_PASSWORD, "123456");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.221:9094");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap-myproject.apps.jscholz.rhmw-integrations.net:31313");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/truststore.jks");
        //props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        //props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        //props.put("security.protocol", "SASL_SSL");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("test-topic"));

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