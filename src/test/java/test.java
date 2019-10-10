import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Random;

public class test {

    static void parquetWriter(String outPath) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {" +
                " required int32 id ;\n" +
                " required binary name ;\n" +
                " required int32 age ;\n" +
                "}");
        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(outPath);
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, configuration);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);
        //把本地文件读取进去，用来生成parquet格式文件
        Random r = new Random();
        Group group = factory.newGroup()
                .append("id", 2)
                .append("name", "xiaohua")
                .append("age",18);
        writer.write(group);
        group = factory.newGroup()
                .append("id", 2)
                .append("name", "xiaohua")
                .append("age",18);
        writer.write(group);
        System.out.println("write end");
        writer.close();
    }

    public static void main(String[] args) {
//        try {
//            ParquetStruct ps = ParquetUtils.parquetWriter(new Path("hdfs://10.16.26.203:8020/user/hive/warehouse/spark_stream_platform.db/student/parquet"));
//            Group group = ps.factory.newGroup().append("id", 2)
//                .append("name", "xiaohua")
//                .append("age",18);
//            ps.writer.write(group);
//            ps.writer.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
