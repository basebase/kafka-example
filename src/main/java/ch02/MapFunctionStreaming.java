package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/17 11:10:49
 * @Description:    kafka map function处理例子
 *
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-output
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-output
 *
 *
 * ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic java-example-source
 *
 *
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic java-example-output --property print.key=true
 *
 *
 */
public class MapFunctionStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-client");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. 读取对应主题数据, 转换成stream
        KStream<byte[], String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.ByteArray(), Serdes.String()));

        // 2. 使用mapValues
        KStream<byte[], String> upperMapValues = source.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                return value.toUpperCase();
            }
        });

        upperMapValues.to("java-example-output", Produced.with(Serdes.ByteArray(), Serdes.String()));


        // 2. 相当于mapValues变体, 只修改value不修改key
        KStream<byte[], String> upperMap = source.map(new KeyValueMapper<byte[], String, KeyValue<byte[], String>>() {
            @Override
            public KeyValue<byte[], String> apply(byte[] key, String value) {
                return new KeyValue<>(key, value.toUpperCase());
            }
        });


        // 3. 使用map修改key和value
        KStream<String, String> map = source.map(new KeyValueMapper<byte[], String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(byte[] key, String value) {
                return new KeyValue<>(value, value.toUpperCase());
            }
        });

        map.to("java-example-output", Produced.with(Serdes.String(), Serdes.String()));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe());
    }
}
