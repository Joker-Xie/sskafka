import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class WriteToHdfs {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://10.16.26.203:8020"),conf);
            FSDataOutputStream fos = fs.create(new Path("/user/hive/warehouse/spark_stream_platform.db/test/log.txt"));
            fos.write("hello hdfs!".getBytes());
            fos.flush();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
