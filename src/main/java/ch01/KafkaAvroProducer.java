package ch01;

import kafka.avro.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/11 14:31:06
 * @Description:
 */
public class KafkaAvroProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        String schemaUrl = "http://0.0.0.0:8081";
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaUrl);
        String topic = "java-example";

        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(props);


        for (int i = 1; i <= 100; i++) {
            User u = new User();
            u.setScore(i * 10);
            u.setAge(1 * 5);

            if (i % 2 == 0) {
                u.setName("zhangsan-" + i);
                u.setCity("hangzhou-" + i);
            } else {
                u.setName("zhangsan-" + i);
            }

            ProducerRecord<String, User> record = new ProducerRecord(topic, u.getName(), u);
            producer.send(record);
        }
    }
}
