package com.meicloud.saprk.hdfs.utils;


import com.meicloud.spark.entity.CaseVo;
import com.meicloud.spark.error.Regular4ErrorData;
import com.meicloud.spark.utils.StringUtils;

import java.util.TimerTask;
/**
 *
 * 定时将数据刷写进入hdfs与更新的异常数据的总条数
 */
public class CloseFSDataStream extends TimerTask {
    private CaseVo.InputKafkaConfigVo inputKafkaConfigVo;
    private CaseVo.JobConfigVo jobConfigVo;

    public  CloseFSDataStream(CaseVo.JobConfigVo jobConfigVo, CaseVo.InputKafkaConfigVo inputKafkaConfigVo) {
        this.inputKafkaConfigVo = inputKafkaConfigVo;
        this.jobConfigVo = jobConfigVo;
    }

    @Override
    public void run() {
        FSDataStreamPool pool = FSDataStreamPool.getInstance();
        pool.releasePool();
        Long errorNum = Regular4ErrorData.getErrorDataNum(jobConfigVo.jobName(),inputKafkaConfigVo.subscribeContent());
        Regular4ErrorData.regularWatcher(jobConfigVo.jobName(),errorNum);
    }
}
