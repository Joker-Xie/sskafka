package com.meicloud.flink.stream.utils;

import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;

public class CkeckPointBuild {


    /**
     * 创建RocksDB 作为状态后端
     *
     * @param flink
     * @param jobConfigVo
     */
    public static void setRocksDBStateBackend(Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink, JobConfigVo jobConfigVo) {
        StreamExecutionEnvironment env = flink.f0;
        //TODO 暂定1分钟写一次checkpoint 使用精准一次进行处理
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        String hdfsURL = "file:///E:/state/"+CommonStreamUtils.getCheckpointLocation(jobConfigVo);
        RocksDBStateBackend rdb = null;
        try {
            rdb = new RocksDBStateBackend(hdfsURL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        env.setStateBackend(rdb);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    }

    /**
     * 创建FsStateBackend 作为状态后端
     *
     * @param flink
     * @param jobConfigVo
     */
    public static void setFsStateBackend(Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink, JobConfigVo jobConfigVo) {
        StreamExecutionEnvironment env = flink.f0;
        //TODO 暂定1分钟写一次checkpoint 使用精准一次进行处理
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        String hdfsURL = CommonStreamUtils.getCheckpointLocation(jobConfigVo);
        FsStateBackend fdb = new FsStateBackend(hdfsURL);
        env.setStateBackend(fdb);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    }

    /**
     * 创建MemoryStateBackend 作为状态后端
     *
     * @param flink
     */
    public static void setMemory(Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        StreamExecutionEnvironment env = flink.f0;
        //TODO 暂定1分钟写一次checkpoint 使用精准一次进行处理
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        MemoryStateBackend mdb = new MemoryStateBackend();
        env.setStateBackend(mdb);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    }

}
