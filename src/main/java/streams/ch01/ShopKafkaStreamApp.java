package streams.ch01;

import model.Shop;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;

import java.util.Properties;

/**
 * @Author xiaomoyu
 * @Date: 2022/3/2 17:17:42
 * @Description:    购买行为kafka stream
 *
 * 业务处理流程
 *      1. 读取kafka topic数据源
 *      2. 屏蔽学生id信息
 *      3. 通过购买力奖励对应的积分
 *      4. 记录学生购物信息
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
 *  > {"id": "FKJ10001V1", "name": "zhangsan", "goods": "牙膏", "amount": 15.8, "address": "北京市海淀区家乐福超市", "cashier": "JLF-1", "type": "支付宝", "createtime": "2022-02-22 15:18:22"}
 *  > {"id": "FKJ10001V2", "name": "zhangsan", "goods": "拖鞋", "amount": 55, "address": "浙江省杭州市喜乐多超时", "cashier": "XLD-1", "type": "微信", "createtime": "2022-02-23 17:22:17"}
 *  > {"id": "FKJ10001V3", "name": "zhangsan", "goods": "上衣", "amount": 1015, "address": "江苏省南京市酷酷鱼时尚店", "cashier": "SSY-1", "type": "支付宝", "createtime": "2022-02-23 10:10:52"}
 *  > {"id": "FKJ10001V4", "name": "zhangsan", "goods": "短裤", "amount": 75.2, "address": "浙江省宁波市爱针织", "cashier": "AZZ-1", "type": "信用卡", "createtime": "2022-02-22 09:18:38"}
 *  > {"id": "FKJ10001V5", "name": "zhangsan", "goods": "手套", "amount": 10, "address": "河北省廊坊市精品手工店", "cashier": "SGD-1", "type": "支付宝", "createtime": "2022-02-22 20:55:22"}
 *  > {"id": "FKJ10001V1", "name": "zhangsan", "goods": "篮球鞋", "amount": 2225, "address": "上海市普陀区体育用品专卖店", "cashier": "ZMD-1", "type": "支付宝", "createtime": "2022-02-22 15:18:38"}
 *  > {"id": "FKJ10001V1", "name": "zhangsan", "goods": "跑鞋", "amount": 1115, "address": "北京市东城区跑鞋体验馆", "cashier": "TYG-1", "type": "支付宝", "createtime": "2022-02-22 15:18:55"}
 *  > {"id": "FKJ10001V2", "name": "zhangsan", "goods": "沐浴露", "amount": 335.8, "address": "浙江省杭州市喜乐多超时", "cashier": "XLD-1", "type": "微信", "createtime": "2022-02-23 17:22:22"}
 *
 *
 *
 *
 *
 */
public class ShopKafkaStreamApp {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shop-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "shop-client");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/xiaomoyu/Documents/code/java/kafka-example/stream-state");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Shop> shopSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Shop.class));
        // 1. 屏蔽用户ID信息
        KStream<String, Shop> shopUserStream = builder.stream("code", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> Shop.build(value).make());


        // 2. 获取到每个用户消费金额, 用来判断该用户可以获得多少积分信息
        shopUserStream.groupBy((key, value) -> {
            return value.getId();
        }, Grouped.valueSerde(shopSerde)).aggregate(() -> 0d, (key, value, agg) -> {
            double amount = value.getAmount() + agg;
            return amount;  /* agg后可能是别的类型, 需要使用Materialized确定数据类型, 默认配置使用String, 在读取数据就会出现类型转换异常 */
        }, Materialized.with(Serdes.String(), Serdes.Double())).toStream().peek((k, v) -> {
            System.out.println(k + " 赠送" + (v / 10) + " 积分");
        });



        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        System.out.println(topology.describe());
    }
}
