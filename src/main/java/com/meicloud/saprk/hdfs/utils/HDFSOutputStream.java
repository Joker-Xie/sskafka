package com.meicloud.saprk.hdfs.utils;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

public class HDFSOutputStream extends FSDataOutputStream {
    private FSDataStreamPool pool;
    private FSDataOutputStream out;
    private String path;

    public HDFSOutputStream(FSDataOutputStream out, String path, FSDataStreamPool pool) throws Exception {
        super(out);
        this.pool = pool;
        this.out = out;
        this.path = path;
    }

    @Override
    public void close() {
        try {
            out.close();
            System.out.println("writeStream is closed.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    public void release() {
        pool.putBack(path, this);
    }

}
