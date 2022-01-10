package ch01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/6 17:22:10
 * @Description:    kafka消息消费者
 */
public class JavaConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaConf = new Properties();
        kafkaConf.put("bootstrap.servers", "localhost:9092");
        kafkaConf.put("group.id", "java-example");
        kafkaConf.put("key.deserializer", StringDeserializer.class);
        kafkaConf.put("value.deserializer", StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConf);
        consumer.subscribe(Collections.singletonList("java-example"));


        ConsumerRecords<String, String> datas = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> data : datas) {
            System.out.println("data: " + data.value());
        }
        Thread.sleep(100000);
    }
}
