package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/14 15:01:12
 * @Description:   字符转大写例子
 */
public class UpperCaseCharacterStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper-case-char");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> upperCasedStream = source.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                return value.toUpperCase();
            }
        });

        upperCasedStream.to("java-example-output", Produced.with(Serdes.String(), Serdes.String()));
        upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("test"));

        Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
