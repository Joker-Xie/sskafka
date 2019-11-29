package com.meicloud.flink.stream.utils;


import com.meicloud.spark.entity.CaseVo.*;
import com.meicloud.spark.utils.ConstantUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.HashMap;

public class CommonStreamUtils {


    public static String getCheckpointLocation(JobConfigVo jobConfigVo) {
        return new StringBuilder()
                .append(ConstantUtils.CHECKPOINT_LOCATION())
                .append("_")
                .append(jobConfigVo.jobName())
                .append("_")
                .append(jobConfigVo.version()).toString();
    }

    public static void validateParameter(Tuple5<JobConfigVo, Tuple2<String, InputSourceConfigVo>, Tuple2<String, ExecutorConfigVo>, HashMap<String, OutputSourceConfigVo>, HashMap<String, StaticInputSourceConfigVo>> jobAllConfigTuple) {
        if (jobAllConfigTuple.f0 == null) {
            System.err.println("Usage: 没有配置JOB");
            System.exit(1);
        }
        if (jobAllConfigTuple.f1 == null) {
            System.err.println("Usage: 没有配置输入源");
            System.exit(1);
        }
        if (jobAllConfigTuple.f2 == null) {
            System.err.println("Usage: 没有配置执行层");
            System.exit(1);
        }
        if (jobAllConfigTuple.f3 == null || jobAllConfigTuple.f3.size() == 0) {
            System.err.println("Usage: 没有此配置输出源");
            System.exit(1);
        }
    }


}
