package cz.scholz.kafka.kafkastreams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;


public class StreamProcessor {
    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"); // Hostname verification


        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("TextLinesTopic");
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count("Counts");
        wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();


    }

}
