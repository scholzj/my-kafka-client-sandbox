package cz.scholz.kafka.kafkaclients;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class MessageConsumerOauth {
    private static int timeout = 5000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args)
    {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        //System.setProperty("javax.net.debug", "ssl");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9099");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "system:serviceaccount:myproject:kafka-consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required token=eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJteXByb2plY3QiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlY3JldC5uYW1lIjoia2Fma2EtY29uc3VtZXItdG9rZW4tbnA3YjciLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoia2Fma2EtY29uc3VtZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJhNTE3ODdhMy05MTdlLTRkY2ItYjE0Ni0wNGQxZGZjYjRhZTIiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6bXlwcm9qZWN0OmthZmthLWNvbnN1bWVyIn0.PTslZt1kTAa9PaKVXCOb8hKzNXPD-eogisoC7uYOb7LPhEo8ZODbHskEzJv3wDI2zb7H-LPn_6Cu_G4sPL_pC8zd2n6lr0SPKpgXRcK850RDXrbFOY8j8a8c6bANwe3zY6QkQXPwfxp8rGGZZbgNWZ79RP9bPS0GCjKwHzoixAmjrszOmOsFtRgTn-8rQ7BN3cPA1VG68VLpc_RIsdkSIAbU4Jx-2WVaaF16JDa2ezTzht3jPoLHyPASzjy0QyAWQW2pFg9mEtBa1z3LDkqWfs26rDlKJh8Qy82NRGt5cnggMnnTEp-76tOWQlG1HQ0vsPaL7lI-bqRDea_L1FDBAQ;");
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.mechanism","OAUTHBEARER");
        props.put("sasl.login.callback.handler.class","io.strimzi.kafka.kubernetes.authenticator.KubernetesTokenLoginCallbackHandler");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("kafka-test-apps"));

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