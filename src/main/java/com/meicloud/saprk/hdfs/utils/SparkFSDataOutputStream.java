package com.meicloud.saprk.hdfs.utils;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class SparkFSDataOutputStream extends BufferedWriter {
    private HDFSRawStreamPool pool;
    private BufferedWriter out ;
    private String path;
    public SparkFSDataOutputStream(BufferedWriter out, String path, HDFSRawStreamPool pool) throws Exception{
        super(out);
//        super(null);
        this.pool = pool;
        this.out = out;
        this.path = path;
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(String b) throws IOException {
        out.write(b);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }
    public void release(){
        pool.putBack(path,this);
    }
}
