package com.meicloud.spark.stream

import java.util.Timer

import com.meicloud.saprk.hdfs.utils.{CloseFSDataStream, HDFSRawConsumer, ProcessDataInsertDB}
import com.meicloud.spark.utils.ConstantUtils._
import com.meicloud.spark.entity.CaseVo._
import com.meicloud.spark.stream.util.{CommonStreamUtils, KafkaStreamUtils}
import com.meicloud.spark.udf.UdfUtils
import com.meicloud.spark.utils.SourceDataUtils

import scala.collection.mutable

object SSKafka {

  def start(jobAllConfigTuple: (JobConfigVo, (String, InputSourceConfigVo),
    (String, ExecutorConfigVo), mutable.HashMap[String, OutputSourceConfigVo], mutable.HashMap[String, StaticInputSourceConfigVo])) = {

    // 验证输入、执行、输出参数是否为空
    CommonStreamUtils.validateParameter(jobAllConfigTuple)

    // job全局配置信息
    val jobConfigVo = jobAllConfigTuple._1

    // 判断输入源为kafka
    if (!RESOURCE_KAFKA.equalsIgnoreCase(jobAllConfigTuple._2._1))
      System.exit(-1)

    //抽象输入源转为kafka输入源
    val inputKafkaConfigVo: InputKafkaConfigVo = jobAllConfigTuple._2._2.asInstanceOf[InputKafkaConfigVo]
    // 执行元数据
    val executorConfigVoTuple = jobAllConfigTuple._3
    // 输出
    val outputSourceConfigVoMap = jobAllConfigTuple._4
    //静态流配置信息
    val staticInputSourceConfigVoMap = jobAllConfigTuple._5

    // 初始化Spark Session
    val spark = CommonStreamUtils.getSparkSession(jobConfigVo)
    //隐式转化
    import spark.implicits._
    // 注册UDF函数
    UdfUtils.registerUDF(executorConfigVoTuple, spark)

    val processStateConf = SourceDataUtils.processStateConf(spark)
    //更改job.status的状态为1
    //    ConfigUtils.updateJobConfigStatus(jobConfigVo)
    //    开启定时更新流运行状态
    //        new Timer().schedule(new ProcessDataInsertDB(processStateConf,jobConfigVo,outputSourceConfigVoMap), 0, 150000)
    //        数据处理
    new Thread(new Runnable {
      def run() {
        KafkaStreamUtils.kafkaStreamDealt(jobConfigVo,
          inputKafkaConfigVo,
          executorConfigVoTuple,
          outputSourceConfigVoMap,
          staticInputSourceConfigVoMap,
          spark,
          processStateConf)
      }
    }).start
    //    //开启定时监控将数据刷写进入HDFS
    new Timer().schedule(new CloseFSDataStream(jobConfigVo, inputKafkaConfigVo), 0, 180000)
    //开启异常数据监控通道
    new Thread(new Runnable {
      def run() {
        new HDFSRawConsumer().watchErrorTopic(inputKafkaConfigVo.subscribeContent)
      }
    }).start
  }
}
