package com.meicloud.spark.stream.attachstream

import com.meicloud.spark.entity.CaseVo.StaticInputSourceConfigVo
import org.apache.spark.sql.{DataFrame, SparkSession}

trait AttachStreamInterface {
  /**
    *
    * 初始化附加流的配置参数
    */
  def init(conf: StaticInputSourceConfigVo)

  /**
    *
    * 根据sparkSession获取附加流
    *
    * @param sparkSession
    */
  def getStream(sparkSession: SparkSession, conf: StaticInputSourceConfigVo): DataFrame

}
