package ch01;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/10 16:31:05
 * @Description:
 */
public class StudentSerializer implements Serializer<Student> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO
    }

    @Override
    public byte[] serialize(String topic, Student data) {
        if (data == null)
            return null;

        byte[] bytes = data.toString().getBytes(StandardCharsets.UTF_8);
        return bytes;
    }



    @Override
    public void close() {
        // TODO
    }
}
