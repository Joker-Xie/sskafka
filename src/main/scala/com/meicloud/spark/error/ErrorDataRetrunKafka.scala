package com.meicloud.spark.error

import com.meicloud.spark.config.StreamKafkaConfig
import com.meicloud.spark.entity.CaseVo

/**
  *
  * 用与处理流平台异常数据的回收再处理
  */
object ErrorDataRetrunKafka {

  def main(args: Array[String]): Unit = {
    try {
      start(args(0))
    } catch {
      case e: Exception =>

        System.exit(1)
    }
  }

  /**
    * 调起异常数据重跑程序
    *
    * @param jobName
    */
  def start(jobName: String): Unit = {
    val config = StreamKafkaConfig.getNodeConfigVo(jobName)
    var topic: String = null
    config._2._1 match {
      case "kafka" =>
        //        topic = PropertiesScalaUtils.getString("error_msg_kafka_topic")
        topic = config._2._2.asInstanceOf[CaseVo.InputKafkaConfigVo].subscribeContent + "_error_data"
      case _ =>
        null
    }
    StreamErrorDataConsumer.startConsumer(jobName, topic)
  }
}
