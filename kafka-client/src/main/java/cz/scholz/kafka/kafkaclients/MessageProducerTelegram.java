package cz.scholz.kafka.kafkaclients;

import cz.scholz.kafka.kafkaclients.util.RandomStringGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MessageProducerTelegram {
    private static int count = 50;
    private static int timeTick = 1000;
    private static int messageSize = 1024;

    private static Boolean debug = false;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.86:31313");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord record = new ProducerRecord<String, String>("telegram-replies", "Response");
        record.headers().add("CamelHeader.CamelTelegramChatId", "494314481".getBytes());
        RecordMetadata result = (RecordMetadata) producer.send(record).get();
        System.out.println("-I- sent message to offset " + result.offset() + ": " + record.key() + " / " + record.value());

        producer.close();
    }
}