package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/18 15:38:42
 * @Description:    Hop Window窗口使用
 */
public class HopWindowStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hop-window-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "hop-window-client");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
//        props.put("schema.registry.url", "http://localhost:8081");
//
//
//        SpecificAvroSerde<PlayEvent> playEventAvroSerde = new SpecificAvroSerde<>();
//        playEventAvroSerde.configure(
//                Collections.singletonMap("schema.registry.url",
//                        "http://localhost:8081"), false);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> filter = source.filter(new Predicate<String, String>() {
            @Override
            public boolean test(String key, String value) {
                return value == null || value.split(",").length != 3 ? false : true;
            }
        });

        KGroupedStream<Long, String> groupedStream = filter.groupBy(new KeyValueMapper<String, String, Long>() {
            @Override
            public Long apply(String key, String value) {
                return Long.parseLong(value.split(",")[0]);
            }
        });

        Duration windowSize = Duration.ofSeconds(60);
        Duration advanceSize = Duration.ofSeconds(30);
        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);
        TimeWindowedKStream<Long, String> windowedKStream = groupedStream.windowedBy(hoppingWindow);

        KTable<Long, Long> aggregate = groupedStream.aggregate(new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        }, new Aggregator<Long, String, Long>() {
            @Override
            public Long apply(Long key, String value, Long aggregate) {
                aggregate += 1;
                return aggregate;
            }
        });

        KStream<Windowed<Long>, Long> windowedLongKStream = windowedKStream.aggregate(new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        }, new Aggregator<Long, String, Long>() {
            @Override
            public Long apply(Long key, String value, Long aggregate) {
                return ++ aggregate;
            }
        }).toStream();

        windowedLongKStream.map(new KeyValueMapper<Windowed<Long>, Long, KeyValue<Long, Long>>() {
            @Override
            public KeyValue<Long, Long> apply(Windowed<Long> key, Long value) {
                return new KeyValue<>(key.key(), value);
            }
        }).print(Printed.<Long, Long>toSysOut().withLabel("hop-window"));


//        KStream<Long, Long> result = aggregate.toStream().map(new KeyValueMapper<Windowed<Long>, Long, KeyValue<Long, Long>>() {
//            @Override
//            public KeyValue<Long, Long> apply(Windowed<Long> key, Long value) {
//                Long k = key.key() == null ? 0L : key.key();
//                return new KeyValue<>(k, value);
//            }
//        });


//        aggregate.toStream().print(Printed.<Long, Long>toSysOut().withLabel("hop-window"));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
//        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe());
    }
}
