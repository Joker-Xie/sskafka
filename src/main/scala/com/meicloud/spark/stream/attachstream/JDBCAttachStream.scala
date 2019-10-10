package com.meicloud.spark.stream.attachstream

import java.util.Properties

import com.meicloud.spark.entity.CaseVo.JdbcStaticConfVo
import org.apache.spark.sql.{DataFrame, SparkSession}

object JDBCAttachStream {
  private var props = new Properties()

  /**
    *
    * 初始化附加流的配置参数
    */
  def init(conf: JdbcStaticConfVo): Unit = {

  }

  /**
    *
    * 根据sparkSession获取附加流
    *
    * @param sparkSession
    */
  def getStream(sparkSession: SparkSession, conf: JdbcStaticConfVo): DataFrame = {
    props.put("user", conf.jdbcUser)
    props.put("password", conf.jdbcPass)
    val url = conf.jdbcUrl
    val table = conf.jdbcTableName
    sparkSession.read.jdbc(url, table, props)
  }
}
