package ch01;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Locale;
import java.util.Properties;
import java.util.stream.Stream;

/***
 * @Auth Joker
 * @Date 2022/02/28 22:27:48
 * @Description     kafka stream 转换字符为答谢
 */
public class YellingApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "yelling-client");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("code", Consumed.<String, String>with(Serdes.String(), Serdes.String()))
                .mapValues(val -> val.toUpperCase())
                .peek((k, v) -> System.out.println("k: " + k + " v: " + v));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
