package com.meicloud.saprk.hdfs.utils;


import com.meicloud.spark.utils.PropertiesScalaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * hdfs写入器
 * */
public class HDFSWriter {

    private String prePath = "";
    private HDFSOutputStream out = null;
    public static final Logger LOGGER = LoggerFactory.getLogger(HDFSWriter.class);
    private String uri = PropertiesScalaUtils.getString("error_msg_hdfs_uri");

    /**
     * @param path hive在hdfs的路径
     * @param log  msg
     */
    public void writeLog2HDFS(String path, String log) {
        try {
            if (!prePath.equals(path)) {
                if (out != null || !"".equals(prePath)) {
                    out.close();
                    out = null;
                }
                out = (HDFSOutputStream) FSDataStreamPool.getInstance().takeOutputStream(path);
                prePath = path;
            }
            if (null == out) {
                out = (HDFSOutputStream) FSDataStreamPool.getInstance().takeOutputStream(path);
            }
            out.write((log + "\n").getBytes());
            out.release();
        } catch (Exception e) {
            LOGGER.info("data flush into HDFS");
        }
    }
}
