package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * @Author: xiaomoyu
 * @Date: 2022/01/31 10:36:51
 * @Description: 数据序列化JSON格式
 */
public class JsonSerializer<T> implements Serializer<T> {

    private Gson gson ;

    public JsonSerializer() {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes(Charset.forName("UTF-8"));
    }


    @Override
    public void close() {

    }
}
