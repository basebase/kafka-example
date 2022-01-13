package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/12 11:33:33
 * @Description:    利用kafka实现wordcount流式应用
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-source
 * ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic java-example-output
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-source
 *
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 * --create --partitions 1 --replication-factor 1 --topic java-example-output
 *
 * ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic java-example-source
 *
 * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic java-example-output
 *
 */
public class WordCountStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-word-count-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        String sourceTopic = "java-example-source";
        String targetTopic = "java-example-output";

        StreamsBuilder builder = new StreamsBuilder();
        // 这里需要指定类型, 否则会报错
        // https://stackoverflow.com/questions/62115660/kafka-streams-exception-org-apache-kafka-streams-errors-streamsexception-dese
        KStream<String, String> source = builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Long> flatMapResult = source.flatMap(new KeyValueMapper<String, String, List<KeyValue<String, Long>>>() {
            @Override
            public List<KeyValue<String, Long>> apply(String key, String value) {
                String[] datas = value.split(",");
                List<KeyValue<String, Long>> resultList = new ArrayList<>();
                for (String data : datas) {
                    if (data.equals("") || data == null)
                        continue;
                    resultList.add(new KeyValue<>(data, 1L));
                }
                return resultList;
            }
        });

        KGroupedStream<String, Long> groupedStream = flatMapResult.groupBy(new KeyValueMapper<String, Long, String>() {

            @Override
            public String apply(String key, Long value) {
                StringBuffer buffer = new StringBuffer();
                buffer.append("key: ").append(key).append("===")
                        .append("value: ").append(value);
                System.out.println(buffer);
                return key;
            }
        });


        groupedStream.aggregate(new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        }, new Aggregator<String, Long, Long>() {
            @Override
            public Long apply(String key, Long value, Long aggregate) {
                System.out.println("key: " + key + " value: " + value + " aggregate: " + aggregate);
                return value + aggregate;
            }
        }).toStream().to(targetTopic);






        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
