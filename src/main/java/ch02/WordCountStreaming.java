package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/14 15:34:54
 * @Description:   利用kafka实现wordcount流式应用
 *
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-output
 *
 *
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-output
 *
 *
 * ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic java-example-source
 *
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic java-example-output \
 * --property print.key=true \
 * --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 *
 */
public class WordCountStreaming {
    public static void main(String[] args) {
        // 1. 初始化配置
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "word-count-client");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 测试用: 禁止使用缓存
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // 配置状态目录, 完成测试后可以删除
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");

        // 2. 创建StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();



        // 3. 逻辑处理

        // 3.1 创建stream指定kv的序列化
        KStream<String, String> stream =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));


        // 3.2 数据打平
        KStream<String, String> flatMapValue = stream.flatMapValues(new ValueMapper<String, List<String>>() {
            @Override
            public List<String> apply(String value) {
                String[] values = value.split(",");
                return Arrays.asList(values);
            }
        });

        // 3.3 数据分组
        KGroupedStream<String, String> groupValue = flatMapValue.groupBy(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                System.out.println("key: " + key + " value: " + value);
                return value;
            }
        });


        // 3.4 数据聚合
        KTable<String, Long> table = groupValue.count();

        System.out.println("table: " + table);


        // 需要在这里进行转换, 否则写不进指定的分区
        KStream<String, Long> out = table.toStream(Named.as("java-example-stream-out"));

        out.to("java-example-output", Produced.with(Serdes.String(), Serdes.Long()));
        out.print(Printed.<String, Long>toSysOut().withLabel("wordcount"));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe());
    }
}
