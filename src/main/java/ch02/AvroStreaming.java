package ch02;

//import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.avro.PlayEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/17 13:53:30
 * @Description:    读取avro数据
 */
public class AvroStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-window-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "session-window-client");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");


//        SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
//        playEventSerde.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
//                "http://localhost:8081"), false);


        StreamsBuilder builder = new StreamsBuilder();
        // 1. 创建数据源
        KStream<String, PlayEvent> source = builder.stream("java-example-source2");

        KStream<String, PlayEvent> filter = source.filter(new Predicate<String, PlayEvent>() {
            @Override
            public boolean test(String key, PlayEvent value) {
                return value.getId() % 2 == 0 ? true : false;
            }
        });

        filter.print(Printed.toSysOut());

//        source.print(Printed.<String, PlayEvent>toSysOut().withLabel("avro"));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe());
    }
}
