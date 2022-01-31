package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * @Author: xiaomoyu
 * @Date: 2022/01/31 10:45:28
 * @Description:    数据反序列化JSON格式
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private Gson gson ;
    private Class<T> deserializedClass;
    private Type reflectionTypeToken;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
        init();

    }

    public JsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
        init();
    }

    private void init () {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (deserializedClass == null) {
            deserializedClass = (Class<T>) configs.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {

        if(bytes == null){
            return null;
        }

        Type deserializeFrom = deserializedClass != null ? deserializedClass :
                reflectionTypeToken;

        return gson.fromJson(new String(bytes),deserializeFrom);
    }



    @Override
    public void close() {

    }
}
