package ch02;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/12 11:33:33
 * @Description:    利用kafka实现wordcount流式应用
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
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        String sourceTopic = "java-example-source";
        String targetTopic = "java-example-output";

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(sourceTopic);

        source.flatMap(new KeyValueMapper<String, String, Iterable<? extends KeyValue<String, String>>>() {
            @Override
            public Iterable<? extends KeyValue<String, String>> apply(String key, String value) {
                String[] datas = value.split(",");
                List<KeyValue<String, String>> resultList = new ArrayList<>();
                for (String data : datas) {
                    resultList.add(new KeyValue<>(data, "1"));
                }
                return resultList;
            }
        }).to(targetTopic);


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
