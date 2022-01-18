package ch02;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.avro.PlayEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/17 18:44:48
 * @Description:    kafka reduce数据聚合
 *
 *
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source2
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source2
 *
 *
 * ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic java-example-source
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic java-example-source2 --property print.key=true
 *
 */
public class ReduceStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "reduce-client");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");

        Serde<PlayEvent> playEventAvroSerde = new SpecificAvroSerde();
        playEventAvroSerde.configure(
                Collections.singletonMap("schema.registry.url",
                "http://localhost:8081"), false);



        StreamsBuilder builder = new StreamsBuilder();
        // 1. 读取数据
        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));


        // 2. 数据打平
        KStream<String, PlayEvent> flatMap = source.flatMap(new KeyValueMapper<String, String, List<KeyValue<String, PlayEvent>>>() {
            @Override
            public List<KeyValue<String, PlayEvent>> apply(String key, String value) {
                String[] tokens = value.split(",");
                List<KeyValue<String, PlayEvent>> result = new ArrayList<>();
                if (tokens.length == 3) {
                    result.add(new KeyValue<>(key, new PlayEvent(Long.parseLong(tokens[0]), tokens[1], tokens[2])));
                }
                return result;
            }
        });


        // 3. 数据分组
        KGroupedStream<String, PlayEvent> groupBy = flatMap.groupBy(new KeyValueMapper<String, PlayEvent, String>() {
            @Override
            public String apply(String key, PlayEvent value) {
                PlayEvent p = new PlayEvent();
                String k = p.getId() + "-" + p.getCreatetime();
                return k;
            }
        });


        KTable<String, PlayEvent> reduce = groupBy.reduce(new Reducer<PlayEvent>() {
            @Override
            public PlayEvent apply(PlayEvent value1, PlayEvent value2) {
                // value1即上一次返回的值
                value2.setType(value2.getType() + "-");
                return value2;
            }
        });


        reduce.toStream().to("java-example-source2", Produced.with(Serdes.String(), playEventAvroSerde));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe());
    }
}
