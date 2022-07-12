package cz.scholz.kafka.kafkaclients;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.scholz.kafka.kafkaclients.util.RandomStringGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MessageProducerFromFile {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.72:32082");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        BufferedReader br = new BufferedReader(new FileReader("/Users/scholzj/development/my-kafka-client-sandbox/kafka-client/avfc-brentford-tweets.txt"));

        String line;
        int lineCount = 0;
        long previousTimestamp = 0;
        int timeFactor = 4;
        String targetTopic = "avfc-search";
        ObjectMapper decoder = new ObjectMapper();
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");

        while ((line = br.readLine()) != null) {
            long createdAt = decoder.readTree(line).get("createdAt").asLong();
            System.out.println("-I- We are at " + formatter.format(new Date(createdAt)) + " ");

            if (previousTimestamp != 0) {
                long sleep = (createdAt - previousTimestamp) / timeFactor;
                System.out.println("-I- Sleeping for " + sleep + " ms");
                Thread.sleep(sleep);
            }

            ProducerRecord record = new ProducerRecord<String, String>(targetTopic, line);
            producer.send(record);
            lineCount++;
            previousTimestamp = createdAt;
        }

        System.out.println("-I- ####################################");
        System.out.println("-I- Done after " + lineCount + " lines");
    }


}