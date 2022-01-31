package ch02;

import model.Student;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import utils.JsonDeserializer;
import utils.JsonSerializer;

import java.util.Properties;

/**
 * @Author: xiaomoyu
 * @Date: 2022/01/31 10:50:38
 * @Description:    自定义Serde
 *
 * 张三,11,北京,1001
 * 李四,22,上海,1002
 * 王五,13,深圳,1003
 * 赵柳,44,广州,1004
 * 狂刀,122,杭州,1005,
 * 关雎,1,广西,1001
 */
public class JsonSerdeStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-serde-app");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/Joker/Documents/my_code/java_code/kafka-example/stream-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        // 自定义序列化, 并注册到Serde
        JsonSerializer<Student> studentJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Student> studentJsonDeserializer = new JsonDeserializer<Student>(Student.class);
        Serde<Student> studentSerde = Serdes.serdeFrom(studentJsonSerializer, studentJsonDeserializer);


        KStream<String, String> source =
                builder.stream("java-example-source", Consumed.with(Serdes.String(), Serdes.String()));
        source.mapValues(value -> new Student(value))
                .to("java-example-source2", Produced.with(Serdes.String(), studentSerde));

        builder.stream("java-example-source2", Consumed.with(Serdes.String(), studentSerde))
                .print(Printed.<String, Student>toSysOut().withLabel("student"));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
