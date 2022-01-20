package ch02;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.avro.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.WindowStore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/20 16:08:30
 * @Description:   等待窗口关闭后发送消息
 *
 *
 * K-001,insert,2021-12-11 11:11:00
 * K-001,insert,2021-12-11 11:11:08
 * K-001,insert,2021-12-11 11:11:25
 * K-001,insert,2021-12-11 11:11:50
 * K-001,insert,2021-12-11 11:11:59
 * K-001,insert,2021-12-11 11:12:00
 * K-001,insert,2021-12-11 11:11:02
 * K-001,insert,2021-12-11 11:11:03
 * K-001,insert,2021-12-11 11:12:28
 * K-001,insert,2021-12-11 11:11:04
 * K-001,insert,2021-12-11 11:12:29
 * K-001,insert,2021-12-11 11:11:05
 * K-001,insert,2021-12-11 11:12:30
 * K-001,insert,2021-12-11 11:11:06
 *
 */
public class SuppressWindowStreaming {

    private static class SuppreMessageTime implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            String value = (String) record.value();
            String[] tokens = value.split(",");
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date d = null;
            try {
                d = format.parse(tokens[2]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return d.getTime();
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suppre-window-app");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");

        SpecificAvroSerde<Message> messageAvroSerde = new SpecificAvroSerde<>();
        messageAvroSerde.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"),
                false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream("java-example-source",
                        Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new SuppreMessageTime()));

        KGroupedStream<String, Message> groupedStream = source.map(new KeyValueMapper<String, String, KeyValue<String, Message>>() {
            @Override
            public KeyValue<String, Message> apply(String key, String value) {
                String[] tokens = value.split(",");
                Message p = new Message(tokens[0], tokens[1], tokens[2]);
                return new KeyValue(p.getId(), p);
            }
        }).groupBy(new KeyValueMapper<String, Message, String>() {
            @Override
            public String apply(String key, Message value) {
                return key;
            }
        }, Grouped.with(Serdes.String(), messageAvroSerde));


        groupedStream.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(30)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count-state-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                // 设置suppress这样数据只有完全到达后采取触发窗口
                // 但是设置之后在有该时段窗口数据将不在被计算
                // suppress不会改变类型, 所以不需要设置返回值类型
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream().peek((k, v) -> System.out.println("suppress key : " + k + " value: " + v));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
