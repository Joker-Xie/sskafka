package com.meicloud.flink.conf;

import com.meicloud.spark.entity.CaseVo.*;
import com.meicloud.spark.utils.ConstantUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StreamConfig {
    public static Tuple5<JobConfigVo, Tuple2<String, InputSourceConfigVo>, Tuple2<String, ExecutorConfigVo>, HashMap<String, OutputSourceConfigVo>, HashMap<String, StaticInputSourceConfigVo>> getNodeConfigVo(String jobName) {
        JobConfigVo jobConfigVo = ConfigUtils.getJobConfigVo(jobName);
        Tuple2<String, InputSourceConfigVo> inputSourceConfigVoTuple = null;
        HashMap staticInputSourceConfigVoMap = new HashMap<String, StaticInputSourceConfigVo>();
        Tuple2<String, ExecutorConfigVo> executorConfigVoTuple = null;
        //节点信息
        List<Tuple3<String, String, String>> list = ConfigUtils.getNodeConfigVo(jobName);
        HashMap outputSourceConfigVoMap = new HashMap<String, OutputSourceConfigVo>();
        for (Tuple3 line : list) {
            String nodeType = line.f0.toString().trim();
            String sourceType = line.f1.toString().trim();
            String sourceId = line.f2.toString().trim();
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.out.println("nodeType:" + nodeType + ",sourceType:" + sourceType + ",sourceId:" + sourceId);
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            if (ConstantUtils.NODE_INPUT().equals(nodeType)) {
                if (ConstantUtils.RESOURCE_KAFKA().equals(sourceType)) {
                    inputSourceConfigVoTuple = new Tuple2<>(ConstantUtils.RESOURCE_KAFKA(), ConfigUtils.getInputKafkaConfig(sourceId));
                }
            } else if (ConstantUtils.NODE_EXECUTE().equals(nodeType)) {
                //TODO sql execute未处理
                if (ConstantUtils.EXECUTE_SQL().equals(sourceType)) {
                    executorConfigVoTuple = new Tuple2(ConstantUtils.EXECUTE_SQL(), ConfigUtils.getExecuteSqlConfig(sourceId));
                }
            } else if (ConstantUtils.NODE_OUTPUT().equals(nodeType)) {
                if (ConstantUtils.RESOURCE_ES().equals(sourceType)) {
                    outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_ES(), ConfigUtils.getOutputEsConfig(sourceId));
                } else if (ConstantUtils.RESOURCE_JDBC().equals(sourceType)) {
                    outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_JDBC(), ConfigUtils.getOutputJdbcConfig(sourceId));
                } else if (ConstantUtils.RESOURCE_FILE().equals(sourceType)) {
                    outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_FILE(), ConfigUtils.getOutputFileConfig(sourceId));
                } else if (ConstantUtils.RESOURCE_KAFKA().equals(sourceType)) {
                    outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_KAFKA(), ConfigUtils.getOutputKafkaConfig(sourceId));
                }
            } else if (ConstantUtils.NODE_STATICINPUT().equals(nodeType)) {
                ArrayList<JdbcStaticConfVo> newList = ConfigUtils.getStaticInputJdbcConfig(sourceId);
                for (int i = 0; i < newList.size(); i++) {
                    JdbcStaticConfVo row = newList.get(i);
                    if (ConstantUtils.RESOURCE_JDBC().equals(row.sourceType())) {
                        staticInputSourceConfigVoMap.put(ConstantUtils.RESOURCE_JDBC() + "_" + i, row);
                    } else if (ConstantUtils.RESOURCE_KAFKA().equals(row.sourceType())) {
                        //              staticInputSourceConfigVoMap = (ConstantUtils.RESOURCE_KAFKA,ConfigUtils.getStaticInputKafkaConfig(sourceId))
                    }
                }
            }
        }
        return new Tuple5<>(jobConfigVo, inputSourceConfigVoTuple, executorConfigVoTuple, outputSourceConfigVoMap, staticInputSourceConfigVoMap);
    }
}
