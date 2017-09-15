package cz.scholz.kafka.kafkaclients;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import cz.scholz.kafka.kafkaclients.util.RandomStringGenerator;

public class MessageProducerCompaction {
    private static int count = 100000;
    private static int messageSize = 1024;

    private static String[] keys = new String[] {"userA", "userB", "userC", "userD", "userE", "userF", "userG", "userH", "userI", "userJ"};

    private static Boolean debug = false;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        Date totalStartTime = new Date();
        int messageNo = 0;
        AtomicInteger inFlightMessagesCounter = new AtomicInteger(0);

        while (messageNo < count)
        {
            messageNo++;

            ProducerRecord record = new ProducerRecord<String, String>("myCompactedTopic", keys[new Random().nextInt(keys.length)], RandomStringGenerator.getSaltString(messageSize));
            producer.send(record, new AsyncProducerCallback(messageNo, debug, inFlightMessagesCounter));

            if (debug)
            {
                System.out.println("-I- sent message no. " + messageNo + ": " + record.key() + " / " + record.value());
            }
        }

        while (inFlightMessagesCounter.get() != 0)
        {
            System.out.println("-W- Waiting for callbacks to complete (in flight callbacks: " + inFlightMessagesCounter + ")");
            Thread.sleep(1000);
        }

        Date totalEndTime = new Date();

        producer.close();

        System.out.println("-I- ####################################");
        System.out.println("-I- Total " + messageNo + " messages sent: avg " + ((float)messageNo/(totalEndTime.getTime()-totalStartTime.getTime())*1000) + " msg/s");
    }
}