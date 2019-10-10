package com.meicloud.saprk.hdfs.utils;

import com.meicloud.spark.utils.PropertiesScalaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import static com.meicloud.saprk.hdfs.utils.JavaStringUtils.ts2yyyyMMdd;

public class FSDataStreamPool {
    private static FileSystem fs;
    private static Map<String, FSDataOutputStream> pool = new HashMap<String, FSDataOutputStream>();
    private static FSDataStreamPool instance;
    private Path path;

    public static FSDataStreamPool getInstance() {
        if (instance == null) {
            instance = new FSDataStreamPool();
        }
        return instance;
    }

    private FSDataStreamPool() {
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

    public synchronized HDFSOutputStream takeOutputStream(String file) {
        FSDataOutputStream out = null;
        Path fsPath = new Path(file);
        try {
            if (pool.size() != 0) {
                out = pool.remove(file);
            }
            /*
             * 判断是不是有流存在
             * 注意，生产环境下不要使用append,append存在不稳定性，特别是是在池化模式下
             * */
            if (out == null) {
                if (fs.exists(fsPath)) {
                    out = new FSDataOutputStream(fs.append(fsPath));
                } else {
                    out = new FSDataOutputStream(fs.create(fsPath, false
                    ));
                }
            }
            return new HDFSOutputStream(out, file, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //回收流
    public synchronized void putBack(String path, FSDataOutputStream out) {
        pool.put(path, out);
    }

    //释放流
    public synchronized void releasePool() {
        try {
            for (FSDataOutputStream o : pool.values()) {
                o.hsync();
                o.hflush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
