package com.meicloud.flink.stream.utils;

import com.meicloud.spark.entity.CaseVo.*;
import com.meicloud.spark.utils.ConstantUtils;

public class CommonStreamUtils {


    public static String getCheckpointLocation(JobConfigVo jobConfigVo) {
        return new StringBuilder().append(ConstantUtils.CHECKPOINT_LOCATION())
                .append(jobConfigVo.jobName())
                .append("_")
                .append(jobConfigVo.version()).toString();
    }
}
