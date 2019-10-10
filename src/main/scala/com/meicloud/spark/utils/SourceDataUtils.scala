package com.meicloud.spark.utils

import java.util

import com.meicloud.spark.entity.CaseVo.{JobConfigVo, OutputJdbcConfigVo, ProcessStateVo}
import com.meicloud.spark.utils.{MySQLPoolManager, PropertiesScalaUtils, StringUtils, TriggerUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql._


object SourceDataUtils {

  /**
    * 用于判断输入数据与数据结构是否匹配
    *
    * @param inputData
    */
  def inputSchemaIsMatch(inputData: DataFrame): Unit = {
    var isAllNull = false
    inputData.selectExpr("struct(*) as value").select("value")
    val queuery = inputData.writeStream
      .outputMode("append")
      .foreachBatch {
        (ds: Dataset[Row], _: Long) => {
          ds.foreachPartition(
            partitionRecords => {
              partitionRecords.foreach(
                record => {
                  println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                  println("record: " + record.toString())
                  println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                  isAllNull = StringUtils.isAllNull(record)
                  throw new Exception(isAllNull.toString)
                }
              )
            })
        }
      }.trigger(Trigger.ProcessingTime(2000))
      .start()
    queuery.awaitTermination()
  }


  /**
    * 适用于schema与源数据格式类型不一样,追溯源数据，避免数据丢失
    *
    * @param jobConfigVo
    * @param sourceDF
    * @param processConf
    */
  def errorDataSetOutPut(jobConfigVo: JobConfigVo, sourceDF: DataFrame, processConf: ProcessStateVo, errorType: String, sourTopic: String): Unit = {
    val jobName = jobConfigVo.jobName
    val jobUpdateTime = StringUtils.getTimeNum(jobConfigVo.updateTime)
    val topic = sourTopic + "_error_data"
    import sourceDF.sparkSession.implicits._
    var tmpDF = sourceDF
      .selectExpr("CAST(value_bak AS STRING)")
      .withColumn("job_name", when($"value_bak".isNull, lit(null))
        .otherwise(jobName)
        .cast(StringType))
      .withColumn("job_updatetime", when($"value_bak".isNull, lit(null))
        .otherwise(jobUpdateTime)
        .cast(StringType))
      .withColumn("error_type", when($"value_bak".isNull, lit(null))
        .otherwise(errorType)
        .cast(StringType))
    tmpDF = tmpDF.selectExpr("concat_ws('&',job_name,job_updatetime,error_type,value_bak) as value")
      .select("value")
    tmpDF.writeStream
      .format("kafka")
      .outputMode("Append")
      .option(ConstantUtils.CHECKPOINT_LOCATION, PropertiesScalaUtils.getString("checkpointLocation") + "/" + jobName + "/error1")
      .option(ConstantUtils.KAFKA_TOPIC, topic)
      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, PropertiesScalaUtils.getString("kafka_broker"))
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
  }

  /**
    * 适用于与输出格式的不一致，数据输出
    *
    * @param jobConfigVo
    * @param sourceDF
    * @param processConf
    */
  def errorDataTypeOutPut(jobConfigVo: JobConfigVo, sourceDF: DataFrame, processConf: ProcessStateVo, errorType: String, sourTopic: String, udfNames: util.List[String]): Unit = {
    val jobName = jobConfigVo.jobName
    val jobUpdateTime = StringUtils.getTimeNum(jobConfigVo.updateTime)
    val topic = sourTopic + "_error_data"
    import sourceDF.sparkSession.implicits._
    var cnt = 0
    val nameItor = udfNames.iterator()
    var dropDF = sourceDF
    while (nameItor.hasNext) {
      val name = nameItor.next()
      dropDF = dropDF.drop(name)
    }
    val tmpDF = dropDF
      .select(to_json(struct("*")).as("value_bak"))
      .selectExpr("CAST(value_bak AS STRING)")
      .withColumn("job_name", when($"value_bak".isNull, lit(null))
        .otherwise(jobName)
        .cast(StringType))
      .withColumn("job_updatetime", when($"value_bak".isNull, lit(null))
        .otherwise(jobUpdateTime)
        .cast(StringType))
      .withColumn("error_type", when($"value_bak".isNull, lit(null))
        .otherwise(errorType)
        .cast(StringType))
      .selectExpr("concat_ws('&',job_name,job_updatetime,error_type,value_bak) as value")
      .select("value")
    tmpDF.writeStream
      .format("kafka")
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, PropertiesScalaUtils.getString("checkpointLocation") + "/" + jobName + "/error2")
      .option(ConstantUtils.KAFKA_TOPIC, topic)
      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, PropertiesScalaUtils.getString("kafka_broker"))
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }

  /**
    * 用于判断输出格式与数据库格式是否统一
    *
    * @param outputJdbcConfigVo
    * @param columnDataTypes
    */
  def ackColumCount(outputJdbcConfigVo: OutputJdbcConfigVo, columnDataTypes: Array[DataType]): (String, String) = {
    val conn = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection //从连接池中获取一个连接
    val sql = "select * from " + outputJdbcConfigVo.jdbcTable + " limit 0"
    val meta = conn.prepareStatement(sql).getMetaData
    val num = meta.getColumnCount
    var boolean = true
    val clumNames = StringBuilder.newBuilder
    if (num == columnDataTypes.length) {
      for (i <- 0 until num) {
        if (i == 0) {
          clumNames.append(meta.getColumnName(i + 1))
        }
        else {
          clumNames.append("," + meta.getColumnName(i + 1))
        }
        boolean = boolean && meta.getColumnClassName(i + 1).contains(columnDataTypes(i).toString.replaceAll("Type", ""))
      }
      if (boolean) {
        (null, clumNames.toString())
      }
      else {
        ("DATABASE_TYPE_MISMATCH", clumNames.toString())
      }
    }
    else {
      ("DATABASE_COLUMN_MISMATCH", clumNames.toString())
    }
  }

  /**
    * 创建广播变量
    *
    * @param spark
    */
  def processStateConf(spark: SparkSession): ProcessStateVo = {
    val sc = spark.sparkContext
    val errorNum = sc.longAccumulator("errorNum")
    val totalNum = sc.longAccumulator("totalNum")
    val finishNum = sc.longAccumulator("finishNum")
    new ProcessStateVo(errorNum, totalNum, finishNum)
  }
}
