package cz.scholz.kafka.kafkastreams;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;


public class StreamProcessorUUID {
    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "uuid-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094");
        /*props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.UUID().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.UUID().getClass());*/

        /*props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/user1.keystore");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/jscholz/development/my-kafka-client-sandbox/ssl-ca/keys/truststore");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"); // Hostname verification*/


        StreamsBuilder builder = new StreamsBuilder();
        KStream<UUID, UUID> uuidStream = builder.stream("UUIDTopic");
        /*uuidStream
                .flatMapValues(value -> Arrays.asList(value.toString().split("-")))
                .to(Serdes.UUID(), Serdes.String(),"UUIDBlocks");*/

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();
    }

}
