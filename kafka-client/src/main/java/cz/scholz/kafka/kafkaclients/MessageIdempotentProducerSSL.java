package cz.scholz.kafka.kafkaclients;

import cz.scholz.kafka.kafkaclients.util.RandomStringGenerator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MessageIdempotentProducerSSL {
    private static int count = 1000;
    private static int timeTick = 100;
    private static int messageSize = 1024;

    private static Boolean debug = false;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        //System.setProperty("javax.net.debug", "ssl");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:31670");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        //props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some-id-my-transactional-id");

        props.put("security.protocol", "SSL");
        props.put("config.providers", "secrets");
        props.put("config.providers.secrets.class", "io.strimzi.kafka.KubernetesSecretConfigProvider");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "${secrets:myproject/my-user:user.crt}");
        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "${secrets:myproject/my-user:user.key}");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "${secrets:myproject/my-cluster-cluster-ca-cert:ca.crt}");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Hostname verification

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //producer.initTransactions();

        Date totalStartTime = new Date();
        Date blockStartTime = new Date();
        int messageNo = 0;
        int size = 0;
        int sizeBlock = 0;

        while (messageNo < count)
        {
            messageNo++;

            //producer.beginTransaction();

            ProducerRecord record = new ProducerRecord<String, String>("kafka-test-apps", "MSG-" + messageNo, RandomStringGenerator.getSaltString(messageSize));
            record.headers().add("jakub", "scholz".getBytes(StandardCharsets.UTF_8));
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

            //producer.commitTransaction();
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