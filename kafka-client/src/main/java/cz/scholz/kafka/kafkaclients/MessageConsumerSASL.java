package cz.scholz.kafka.kafkaclients;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class MessageConsumerSASL {
    private static int timeout = 60000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args)
    {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        //System.setProperty("javax.net.debug", "ssl");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:31235");
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "mirroring.servicebus.windows.net:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //props.put("security.protocol", "SASL_SSL");
        //props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/truststore.jks");
        //props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/custom-certificates/ca.truststore");
        //props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        //props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/Users/scholzj/development/strimzi/hacking/user.p12");
        //props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        /*props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");*/

        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","SCRAM-SHA-512");
        props.put("config.providers", "secrets");
        props.put("config.providers.secrets.class", "cz.scholz.kafka.KubernetesSecretConfigProvider");
        props.put("sasl.jaas.config", "${secrets:myproject/my-user2:sasl.jaas.config}");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my-user\" password=\"SFD4mkgAaDhf\";");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "${secrets:myproject/my-cluster-cluster-ca-cert:ca.crt}");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Hostname verification

        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://mirroring.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=O1Nqu6ejbV00iw3r9/NVeavIG10BSmmdVx1hrTCLKfI=\";");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"my-user\" password=\"SFD4mkgAaDhf\";");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"SFD4mkgAaDhf\";");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user2\" password=\"jfSmOyaOHXSg\";");

        //props.put("security.protocol","SASL_PLAINTEXT");
        //props.put("sasl.mechanism","PLAIN");
        //props.put("sasl.mechanism","SCRAM-SHA-512");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //consumer.subscribe(Collections.singletonList("kafka-test-apps"));
        consumer.subscribe(Collections.singletonList("my-cluster-source.kafka-test-apps"));

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
                break;
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

        Date blockEndTime = new Date();
        Date totalEndTime = new Date();

        consumer.close();

        if (messageNo % timeTick != 0) {
            System.out.println("-I- " + messageNo + " messages received: " + ((float)(messageNo % timeTick)/(blockEndTime.getTime()-blockStartTime.getTime()-timeout)*1000) + " msg/s, "	+ ((float) sizeBlock / (blockEndTime.getTime() - blockStartTime.getTime()) * 1000 / 1000) + " kB/s, average size " + ((float)sizeBlock/(float)(messageNo % timeTick)));
        }

        System.out.println("-I- ####################################");
        System.out.println("-I- Total " + messageNo + " messages received: avg " + ((float)messageNo/(totalEndTime.getTime()-totalStartTime.getTime()-timeout)*1000) + " msg/s, " + ((float)size/(totalEndTime.getTime()-totalStartTime.getTime()-timeout)*1000 / 1000) + " kB/s, average size " + ((float)size/(float)messageNo));
    }
}