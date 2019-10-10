package com.meicloud.saprk.hdfs.utils;

import com.meicloud.spark.utils.PropertiesScalaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.meicloud.saprk.hdfs.utils.JavaStringUtils.ts2yyyyMMdd;

public class HDFSRawStreamPool {
    private static FileSystem fs;
    //连接池
    private static Map<String, BufferedWriter> pool = new HashMap<String, BufferedWriter>();
    //单例模式生成池
    private static HDFSRawStreamPool instance;

    private Path path;


    public static HDFSRawStreamPool getInstance() {
        if (instance == null) {
            instance = new HDFSRawStreamPool();
        }
        return instance;
    }

    private HDFSRawStreamPool() {
        try {
            Configuration conf = new Configuration();
            String uri = PropertiesScalaUtils.getString("error_msg_hdfs_uri");
            Long yyyyMMDD = System.currentTimeMillis();
            path = new Path(uri + PropertiesScalaUtils.getString("error_msg_hive_table_addr") + "/data_part_" + ts2yyyyMMdd(yyyyMMDD));
            fs = path.getFileSystem(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized BufferedWriter takeOutputStream(String file) {
        try {
            BufferedWriter out = null;
            if (pool.size() != 0) {
                out = pool.remove(file);
            }
//            if (!fs.exists(path)){
//                path = new Path(PropertiesScalaUtils.getString("error_msg_hdfs_uri")+file);
//            }
            /*
             * 判断是不是有流存在
             * 注意，生产环境下不要使用append,append存在不稳定性，特别是是在池化模式下
             * */
            if (out == null) {
                if (fs.exists(path)){
                    out = new BufferedWriter(new OutputStreamWriter(fs.append(path)), 2048);
                }
                else {
                    out = new BufferedWriter(new OutputStreamWriter(fs.create(path)), 2048);
                }
            }
            return new SparkFSDataOutputStream(out, file, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //回收流
    public void putBack(String path, BufferedWriter out) {
        pool.put(path, out);
    }

    //释放流
    public synchronized void releasePool() {
        try {
            for (BufferedWriter o : pool.values()) {
                o.close();
            }
            pool.clear();
//            System.out.println("writor have been released.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
