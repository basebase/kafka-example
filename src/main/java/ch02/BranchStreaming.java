package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

/**
 * @Author: xiaomoyu
 * @Date: 2022/02/01 20:06:34
 * @Description:    branch使用
 */
public class BranchStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-app");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/Joker/Documents/my_code/java_code/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String>[] branch = source.branch((k, v) -> {
            String[] tokens = v.split(",");
            if (tokens[0].toUpperCase().contains("AK-"))
                return true;
            return false;
        }, (k, v) -> {
            String[] tokens = v.split(",");
            if (tokens[0].toUpperCase().contains("UZI-"))
                return true;
            return false;
        });

        KStream<String, String> akStream = branch[0];
        KStream<String, String> uziStream = branch[1];

        akStream.print(Printed.<String, String>toSysOut().withLabel("ak"));
        uziStream.print(Printed.<String, String>toSysOut().withLabel("uzi"));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
