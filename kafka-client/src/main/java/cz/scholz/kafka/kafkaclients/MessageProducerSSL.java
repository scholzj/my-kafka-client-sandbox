package cz.scholz.kafka.kafkaclients;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;

import cz.scholz.kafka.kafkaclients.util.RandomStringGenerator;

public class MessageProducerSSL {
    private static int count = 1000;
    private static int timeTick = 1000;
    private static int messageSize = 1024;

    private static Boolean debug = false;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"); // Hostname verification

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        Date totalStartTime = new Date();
        Date blockStartTime = new Date();
        int messageNo = 0;
        int size = 0;
        int sizeBlock = 0;

        while (messageNo < count)
        {
            messageNo++;

            ProducerRecord record = new ProducerRecord<String, String>("myTopic", "MSG-" + messageNo, RandomStringGenerator.getSaltString(messageSize));
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
    }


}