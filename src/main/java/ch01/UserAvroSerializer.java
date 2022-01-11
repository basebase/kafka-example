package ch01;

import kafka.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/11 13:44:37
 * @Description:    User对象进行序列化
 */
public class UserAvroSerializer {
    public static void main(String[] args) throws IOException {

        // 可以不设置city字段, 默认为null
        User u1 = new User();
        u1.setName("zhangsan");
        u1.setAge(11);
//        u1.setCity("beijing");
        u1.setScore(100);

        User u2 = new User("lisi", 22, "shanghai", 200);

        // 如果使用Builder的形式, 则需要把每个字段都写上, 否则抛出异常
        User u3 = User.newBuilder()
                .setName("wangwu")
                .setAge(33)
                .setScore(300)
                .setCity(null)
                .build();

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);

        dataFileWriter.create(u1.getSchema(), new File("users.avro"));
        dataFileWriter.append(u1);
        dataFileWriter.append(u2);
        dataFileWriter.append(u3);


        dataFileWriter.flush();
        dataFileWriter.close();
    }
}
