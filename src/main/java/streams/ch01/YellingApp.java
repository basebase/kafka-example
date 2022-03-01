package streams.ch01;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/3/1 15:54:07
 * @Description:   kafka stream 将数据转换为大写
 *
 * kafka stream 构建流程
 *  1. 创建所需的配置, 即: StreamsConfig
 *  2. 创建反序列化对象(Serde), 从topic中读取数据需要指定对应的反序列化否则会报错
 *  3. 创建Topology
 *  4. 启动kafka stream程序
 *
 *
 *  ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic code
 *
 *  ./kafka-topics.sh --bootstrap-server localhost:9092 \
 *  --create --partitions 1 --replication-factor 1 --topic code
 *
 *  ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic code
 *
 *  ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic code --property print.key=true
 *
 *
 *  > wo cao wu qing
 *  > test kafka stream example
 *  > hello world
 *
 */
public class YellingApp {
    public static void main(String[] args) {
        Properties props = new Properties();

        // 1. 创建kafka配置
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "yelling-client");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        // 1. 创建数据源, 限制类型为String
        builder.stream("code", Consumed.<String, String>with(Serdes.String(), Serdes.String()))
                .mapValues(value -> value.toUpperCase())        // 2. 将数据转换为大写
                .peek((k, v) -> System.out.println("k: " + k + " v: " + v));        // 3. 将转换后的数据输出

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        // 4. 启动kafka stream程序
        streams.start();
        System.out.println(topology.describe());
    }
}
