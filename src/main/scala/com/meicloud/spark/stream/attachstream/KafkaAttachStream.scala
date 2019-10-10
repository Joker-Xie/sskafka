package com.meicloud.spark.stream.attachstream

import com.meicloud.spark.entity.CaseVo.KafkaStaticConfVo
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaAttachStream {
  /**
    *
    * 初始化附加流的配置参数
    */
  def init(conf: KafkaStaticConfVo): Unit = {}

  /**
    *
    * 根据sparkSession获取附加流
    *
    * @param sparkSession
    */
  def getStream(sparkSession: SparkSession, conf: KafkaStaticConfVo): DataFrame = {
    null
  }
}
