package cz.scholz.kafka.kafkaclients;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageConsumer {
    private static int timeout = 30000;
    private static int timeTick = 1000;

    private static Boolean debug = true;

    public static void main(String[] args)
    {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");

        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.86:31175");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap.myproject:9095");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "macbook4");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //consumer.subscribe(Collections.singletonList("kafka-test-apps"));
        //consumer.subscribe(Pattern.compile(".*kafka-test-apps"));
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
                    System.out.println("-I- received message no. " + messageNo + ": " + record.key() + " / " + record.value() + " (offset: " + record.offset() + ")");
                }

                record.headers().forEach(header -> {
                    System.out.println("-I- Header ... key: " + header.key() + "; value: " + new String(header.value()));
                });

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