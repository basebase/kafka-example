package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/21 14:18:09
 * @Description:    KTable 需要在product中传入key值, 否则出现警告信息: skipping record due to null key
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source
 *
 * ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic java-example-source \
 *  --property "parse.key=true" --property "key.separator=:"
 *
 *
 *  K-001:K-001,insert,2021-12-11 11:11:00
 *
 */
public class KTableStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ktable-client");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");
        StreamsBuilder builder = new StreamsBuilder();

//        SpecificAvroSerde<Message> messageAvroSerde = new SpecificAvroSerde<>();
//        messageAvroSerde.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"), false);

        builder.table("java-example-source",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-source-store")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                .filter((k, v) -> v != null && v.split(",").length == 3)
//                .mapValues(new ValueMapper<String, Message>() {
//                    @Override
//                    public Message apply(String value) {
//                        String[] tokens = value.split(",");
//                        return new Message(tokens[0], tokens[1], tokens[2]);
//                    }
//                })
                .toStream().to("java-example-source2", Produced.with(Serdes.String(), Serdes.String()));//.peek((k, v) -> System.out.println("k: " + k + " v: " + v));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
