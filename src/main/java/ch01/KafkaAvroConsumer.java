package ch01;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.avro.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/11 16:01:51
 * @Description:
 */
public class KafkaAvroConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        String schemaUrl = "http://0.0.0.0:8081";
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "java-example-group");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", schemaUrl);

        // 需要加上该配置, 否则会抛出异常
        props.put("specific.avro.reader", true);


        String topic = "java-example";
        KafkaConsumer<String, User> consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, User> record : records) {
                User u = record.value();
                System.out.println(u);
            }
        }
    }
}
