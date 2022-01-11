package ch01;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/6 17:21:43
 * @Description:    kafka消息生产者
 *
 * ./kafka-topics.sh --bootstrap-server localhost:9092 \
 *   --create --partitions 1 --replication-factor 1 --topic java-example
 */
public class JavaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 基于String类型的序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义序列化
//        props.put("value.serializer", "ch01.StudentSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer(props);
//        KafkaProducer<String, Student> producer = new KafkaProducer(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("java-example", "java-key", "java-value");

//        ProducerRecord<String, Student> record =
//                new ProducerRecord<>("java-example", "java-key", new Student(1, "zhangsan"));


        try {
            // 忽略发送返回值, 无法得知是否成功发送到服务端
            producer.send(record);

            // 同步发送, 如果服务器返回错误, get方法会返回异常
            RecordMetadata metadata = producer.send(record).get();

            StringBuffer buffer = new StringBuffer("kafka info: ");
            buffer.append("Topic: ").append(metadata.topic()).append("\n")
                    .append("Partition: ").append(metadata.partition()).append("\n")
                    .append("Offset: ").append(metadata.offset()).append("\n")
                    .append("TimeStamp: ").append(metadata.timestamp()).append("\n");
            System.out.println(buffer);

            // 异步发送
            producer.send(record, new ProducerCallback());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


class ProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        e.printStackTrace();
    }
}