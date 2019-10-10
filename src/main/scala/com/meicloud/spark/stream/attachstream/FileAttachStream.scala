package com.meicloud.spark.stream.attachstream

import com.meicloud.spark.entity.CaseVo.FileStaticConfVo
import com.meicloud.spark.utils.{ConstantUtils, SchemaUtils, StringUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FileAttachStream {

  val logger = Logger.getLogger(FileAttachStream.getClass)

  /**
    *
    * 初始化附加流的配置参数
    */
  def init(conf: FileStaticConfVo): Unit = {
    println("xxxx")
  }

  /**
    *
    * 根据sparkSession获取附加流
    *
    * @param sparkSession
    */
  def getStream(sparkSession: SparkSession, conf: FileStaticConfVo): DataFrame = {
    val dataFileType = conf.sourceType.trim.toUpperCase
    val path = conf.filePath
    dataFileType match {
      case "CSV" => {
        sparkSession.read
          .format("csv")
          .option("delimiter", ",")
          .option("header", "true")
          .option("quote", "'")
          .option("nullValue", "\\N")
          .option("inferSchema", "true")
          .load(path)
      }
      case "PARQUENT" => {
        sparkSession.read.parquet(path)
      }
      case "JSON" => {
        val schema = SchemaUtils.str2schema(conf.schema)
        sparkSession.read
          .textFile(path)
          .select(from_json(col("value"), schema, ConstantUtils.JSON_OPTIONS).as("tempTable"))
          .select("tempTable.*")
      }
      case _ => null
    }
  }
}
