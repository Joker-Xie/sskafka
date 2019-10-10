package com.meicloud.spark.stream.util

import java.util.UUID

import com.meicloud.spark.utils.{ConstantUtils, _}
import com.meicloud.spark.entity.CaseVo._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by yesk on 2019-3-10. 
  */
object CommonStreamUtils {

  def validateParameter(jobAllConfigTuple: (JobConfigVo, (String, InputSourceConfigVo),
    (String, ExecutorConfigVo), mutable.HashMap[String, OutputSourceConfigVo], mutable.HashMap[String, StaticInputSourceConfigVo])): Unit = {
    if (jobAllConfigTuple._1 == null) {
      System.err.println("Usage: 没有配置JOB")
      System.exit(1)
    }
    if (jobAllConfigTuple._2 == null) {
      System.err.println("Usage: 没有配置输入源")
      System.exit(1)
    }
    if (jobAllConfigTuple._3 == null) {
      System.err.println("Usage: 没有配置执行层")
      System.exit(1)
    }
    if (jobAllConfigTuple._4 == null || jobAllConfigTuple._4.size == 0) {
      System.err.println("Usage: 没有此配置输出源")
      System.exit(1)
    }
  }

  def getCheckpointLocation(jobConfigVo: JobConfigVo): String = {
    new StringBuffer().append(PropertiesScalaUtils.getString(ConstantUtils.CHECKPOINT_LOCATION))
      .append(jobConfigVo.jobName)
      .append("_")
      .append(jobConfigVo.version.toString).toString
  }

  def getSparkSession(jobConfigVo: JobConfigVo): SparkSession = {
    val spark = SparkSession
      .builder
      //      .master(if (jobConfigVo.master==null || "".equals(jobConfigVo.master)) "yarn" else jobConfigVo.master)
      .master("local")
      .appName(jobConfigVo.jobName)
      //设置shuffle的个数

      .config("spark.yarn.tags", jobConfigVo.jobName)
      .getOrCreate()
    spark.sparkContext.setLogLevel(ConstantUtils.LOG_LEVEL_INFO)
    //    spark.sqlContext.setConf("spark.sql.shuffle.partitions","100")
    spark
  }
}
