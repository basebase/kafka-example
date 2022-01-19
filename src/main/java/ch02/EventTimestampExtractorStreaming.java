package ch02;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.avro.PlayEvent;
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
 * @Date: 2022/1/19 15:43:53
 * @Description:     基于事件时间窗口
 *
 *
 *
 * 1,click,2021-12-15 15:00:00
 * 1,click,2021-12-15 15:00:15
 * 1,click,2021-12-15 15:00:17
 * 1,click,2021-12-15 15:00:29
 * 1,click,2021-12-15 15:00:30
 * 1,click,2021-12-15 15:00:40
 * 1,click,2021-12-15 15:00:59
 * 1,click,2021-12-15 15:01:00
 *
 */
public class EventTimestampExtractorStreaming {

    private static class PlayTimestampExtractor implements TimestampExtractor {

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

            System.out.println("Extracting time of " + d.getTime() + " from " + value);
            return d.getTime();
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-time-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "event-time-client");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");
        StreamsBuilder builder = new StreamsBuilder();

        SpecificAvroSerde<PlayEvent> playEventAvroSerde = new SpecificAvroSerde<>();
        playEventAvroSerde.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"),
                false);



        KStream<String, String> source = builder.stream("java-example-source",
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new PlayTimestampExtractor()));

        TimeWindowedKStream<Long, PlayEvent> windowedKStream = source.map(new KeyValueMapper<String, String, KeyValue<Long, PlayEvent>>() {
            @Override
            public KeyValue<Long, PlayEvent> apply(String key, String value) {
                String[] tokens = value.split(",");
                PlayEvent p = new PlayEvent(Long.parseLong(tokens[0]), tokens[1], tokens[2]);
                return new KeyValue<>(p.getId(), p);
            }
        }).groupBy(new KeyValueMapper<Long, PlayEvent, Long>() {
            @Override
            public Long apply(Long key, PlayEvent value) {
                return key;
            }
        }, Grouped.with(Serdes.Long(), playEventAvroSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)) /* .advanceBy(Duration.ofSeconds(30)) */ );




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
            // 还需要指定一个key/value的序列化类型
        }, Materialized.<Long, Long, WindowStore<Bytes, byte[]>>as("agg-state-store").withKeySerde(Serdes.Long()).withValueSerde(Serdes.Long()));

        aggregate.toStream().print(Printed.<Windowed<Long>, Long>toSysOut().withLabel("agg"));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
