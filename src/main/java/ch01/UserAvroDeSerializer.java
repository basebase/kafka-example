package ch01;

import kafka.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/11 13:53:33
 * @Description:    User对象进行反序列化
 */
public class UserAvroDeSerializer {
    public static void main(String[] args) throws IOException {
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> reader = new DataFileReader<User>(new File("users.avro"), userDatumReader);

        while (reader.hasNext()) {
            User u = reader.next();
            System.out.println(u);
        }
    }
}
