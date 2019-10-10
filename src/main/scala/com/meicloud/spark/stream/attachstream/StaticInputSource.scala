package com.meicloud.spark.stream.attachstream

import java.util

import com.meicloud.spark.entity.CaseVo._
import com.meicloud.spark.utils.ConstantUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  *
  * 加载静态数据类
  */


object StaticInputSource {

  /**
    * 加载与注册方法
    *
    * @param spark
    * @param staticData
    */

  def loadAndRegister(spark: SparkSession, staticData: mutable.HashMap[String, StaticInputSourceConfigVo]): java.util.ArrayList[String] = {
    val tableNameList = new util.ArrayList[String]()
    staticData.map {
      kv: (String, StaticInputSourceConfigVo) => {
        kv._1.split("_")(0) match {
          case ConstantUtils.RESOURCE_KAFKA => {
            val staticKafka: KafkaStaticConfVo = kv._2.asInstanceOf[KafkaStaticConfVo]
            tableNameList.add(staticKafka.tableName)
            KafkaAttachStream.getStream(spark, staticKafka).createOrReplaceGlobalTempView(staticKafka.tableName)
          }
          case ConstantUtils.RESOURCE_JDBC => {
            val staticJdbc: JdbcStaticConfVo = kv._2.asInstanceOf[JdbcStaticConfVo]
            tableNameList.add(staticJdbc.registTableName)
            JDBCAttachStream.getStream(spark, staticJdbc).createOrReplaceGlobalTempView(staticJdbc.registTableName)
          }
          case ConstantUtils.RESOURCE_FILE => {
            val staticFile = kv._2.asInstanceOf[FileStaticConfVo]
            tableNameList.add(staticFile.tableName)
            FileAttachStream.getStream(spark, staticFile).createOrReplaceGlobalTempView(staticFile.tableName)
          }
          case ConstantUtils.RESOURCE_ES => {
            val staticEs = kv._2.asInstanceOf[EsStaticConfVo]
            tableNameList.add(staticEs.tableName)
            EsAttachStream.getStream(spark, staticEs).createOrReplaceGlobalTempView(staticEs.tableName)
          }
          case _ => null
        }
      }
    }
    tableNameList
  }
}
