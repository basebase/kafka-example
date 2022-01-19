package ch02;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.avro.PlayEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * @Author xiaomoyu
 * @Date: 2022/1/19 10:47:08
 * @Description:    kafka Hop Window窗口使用
 *
 *
 *
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source2
 *
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source2
 *
 * ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic java-example-source
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic java-example-source2 --property print.key=true
 *
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
        props.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();

        SpecificAvroSerde<PlayEvent> playEventSpecificAvroSerde = new SpecificAvroSerde<>();
        playEventSpecificAvroSerde.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"),
                false);

        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<Long, PlayEvent> map = source.map(new KeyValueMapper<String, String, KeyValue<Long, PlayEvent>>() {
            @Override
            public KeyValue<Long, PlayEvent> apply(String key, String value) {
                if (value == null || value.split(",").length != 3)
                    return new KeyValue<>(-1L, null);
                String[] tokens = value.split(",");
                return new KeyValue(Long.parseLong(tokens[0]), new PlayEvent(Long.parseLong(tokens[0]), tokens[1], tokens[2]));
            }
        });

        // org.apache.kafka.streams.errors.StreamsException: ClassCastException while producing data to topic hop-window-app-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition. A serializer (key: org.apache.kafka.common.serialization.LongSerializer / value: org.apache.kafka.common.serialization.LongSerializer) is not compatible to the actual key or value type (key type: java.lang.Long / value type: kafka.avro.PlayEvent). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters (for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with `Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).
        // 一开始认为在count()/aggregate()等方法报错, 因为最终的写入都不是PlayEvent类型, 但是错误一直提示我value: PlayEvent类型
        // 后来在groupBy方法后面加入对应的key/value的序列化类型后, 才不在提示, 回过头来看这个bug确实如此, 毕竟只有这个地方使用了
        // 最开始被map方法结果误导, 认为可以写入到topic中是没问题, 但是我没想到groupby写入的时候, 也是要对应的序列化类型, 由于默认是long类型, 所以转换出错
        KGroupedStream<Long, PlayEvent> groupedStream = map.groupBy(new KeyValueMapper<Long, PlayEvent, Long>() {
            @Override
            public Long apply(Long key, PlayEvent value) {
                return key;
            }
        }, /* 添加Grouped.with()指定key/value序列化类型, 或者单独指定key/value */Grouped.with(Serdes.Long(), playEventSpecificAvroSerde));


        // 创建Hop Window
        // 窗口大小为1分钟, 每半分钟滚动一次
        Duration windowSize = Duration.ofSeconds(60);
        Duration advanceSize = Duration.ofSeconds(30);
        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);
        TimeWindowedKStream<Long, PlayEvent> windowedKStream = groupedStream.windowedBy(hoppingWindow);


        // 窗口数据聚合
        KTable<Windowed<Long>, Long> aggregate = windowedKStream.aggregate(new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        }, new Aggregator<Long, PlayEvent, Long>() {
            @Override
            public Long apply(Long key, PlayEvent value, Long aggregate) {
                return ++aggregate;
            }
        }, /* 还是需要指定Materialized, 也就是state状态 */Materialized.as("hop-window"));

        // 打印数据
        aggregate.toStream().print(Printed.toSysOut());


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
