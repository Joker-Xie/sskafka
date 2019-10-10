package com.meicloud.flink;

import com.meicloud.flink.setting.FlinkCommonStreamUtils;
import com.meicloud.flink.udf.UdfUtils;
import com.meicloud.spark.config.StreamKafkaConfig;
import com.meicloud.spark.entity.CaseVo.*;
import com.meicloud.spark.stream.util.CommonStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import com.meicloud.flink.stream.*;
import scala.Tuple2;
import scala.Tuple5;
import scala.collection.mutable.HashMap;

public class FlinkStream {

    public static void start(String jobName) {
        Tuple5<JobConfigVo, Tuple2<String, InputSourceConfigVo>, Tuple2<String, ExecutorConfigVo>, HashMap<String, OutputSourceConfigVo>, HashMap<String, StaticInputSourceConfigVo>>
                jobAllConfigTuple = StreamKafkaConfig.getNodeConfigVo(jobName);
        // 验证输入、执行、输出参数是否为空
        CommonStreamUtils.validateParameter(jobAllConfigTuple);
        // job全局配置信息
        JobConfigVo jobConfigVo = jobAllConfigTuple._1();
        // 判断输入源为kafka
        if (!JavaConstantUtils.RESOURCE_KAFKA.equalsIgnoreCase(jobAllConfigTuple._2()._1)) {
            System.exit(-1);
        }
        //抽象输入源转为kafka输入源
        InputKafkaConfigVo inputKafkaConfigVo = (InputKafkaConfigVo) jobAllConfigTuple._2()._2;
        // 执行元数据
        Tuple2<String, ExecutorConfigVo> executorConfigVoTuple = jobAllConfigTuple._3();
        // 输出
        HashMap<String, OutputSourceConfigVo> outputSourceConfigVoMap = jobAllConfigTuple._4();
        //静态流配置信息
        HashMap<String, StaticInputSourceConfigVo> staticInputSourceConfigVoMap = jobAllConfigTuple._5();

        // 初始化Flink env
        Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flinkTuple = FlinkCommonStreamUtils.getFlinkStreamEnv();

        // 注册UDF函数
//        UdfUtils.registerUDF(executorConfigVoTuple, flinkTuple._2);

        //启动flink程序
        KafkaStreamUtils.kafkaStreamDealt(jobConfigVo,
                inputKafkaConfigVo,
                executorConfigVoTuple,
                outputSourceConfigVoMap,
                staticInputSourceConfigVoMap,
                flinkTuple);

    }

    public static void main(String[] args) {
        start("Mutil_Stream_work");
        System.out.println("xxxxxxxxxx");

    }
}


