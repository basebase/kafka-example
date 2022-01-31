package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

/**
 * @Author: xiaomoyu
 * @Date: 2022/01/30 21:06:26
 * @Description:   通过流处理每个字符串转大写
 */
public class YellingStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "yelling-client");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/Joker/Documents/my_code/java_code/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));

        source.mapValues(value -> value.toUpperCase())
                .print(Printed.toSysOut());
//                .peek((k, v) -> System.out.println());

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
