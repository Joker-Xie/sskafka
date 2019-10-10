package com.meicloud.spark.stream.attachstream

import com.meicloud.spark.entity.CaseVo.EsStaticConfVo
import com.meicloud.spark.utils.ConstantUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object EsAttachStream {
  /**
    *
    * 初始化附加流的配置参数
    */
  def init(conf: EsStaticConfVo): Unit = {}

  /**
    *
    * 根据sparkSession获取附加流
    *
    * @param sparkSession
    */
  def getStream(sparkSession: SparkSession, conf: EsStaticConfVo): DataFrame = {
    sparkSession.read
      .format(ConstantUtils.ORG_ELASTICSEARCH_SPARK_SQL)
      .option(ConstantUtils.ES_NET_HTTP_AUTH_USER, conf.esUser)
      .option(ConstantUtils.ES_NET_HTTP_AUTH_PASS, conf.esPass)
      .option(ConstantUtils.ES_NODES, conf.esNodes)
      .option(ConstantUtils.ES_PORT, conf.esPort)
      .option(ConstantUtils.ES_RESOURCE, conf.esResource)
      .load()
  }
}
