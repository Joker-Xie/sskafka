package com.meicloud.spark.stream.util

import java.util

import com.google.gson.{JsonObject, JsonParser}
import com.meicloud.spark.utils._
import com.meicloud.spark.entity.CaseVo.{OutputFileConfigVo, OutputJdbcConfigVo, _}
import com.meicloud.spark.offset.hbase.OffsetOperate
import com.meicloud.spark.offset.redis.RedisOffsetOperate
import com.meicloud.spark.stream.attachstream.StaticInputSource
import com.meicloud.spark.stream.kafka.{KafkaToFile, KafkaToKafka, KafkaToMysql}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable

/**
  * Created by yesk on 2019-3-10. 
  */
object KafkaStreamUtils {
  // 第一个步骤 :直接跟Kafka建立通道
  def getKafkaInputDataFrame(jobConfigVo: JobConfigVo, inputKafkaConfigVo: InputKafkaConfigVo, spark: SparkSession): DataFrame = {
    // Create DataSet representing the stream of input lines from kafka
    val inputDF = spark
      .readStream
      .format(ConstantUtils.FORMAT_KAFKA)
      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, inputKafkaConfigVo.kafkaBootstrapServers)
      .option(inputKafkaConfigVo.subscribeType, inputKafkaConfigVo.subscribeContent)
    // 默认是快照形式 ，0：快照 1：人为管理offset保存到Hbase
    if (ConstantUtils.FAULT_TOLERANT_HBASE.equalsIgnoreCase(jobConfigVo.faultTolerant))
      inputDF.option(ConstantUtils.STARTING_OFFSETS,
        OffsetOperate.getLastCommittedOffsets(jobConfigVo.jobName,
          inputKafkaConfigVo.subscribeContent,
          jobConfigVo.version))
    else if (ConstantUtils.FAULT_TOLERANT_REDIS.equalsIgnoreCase(jobConfigVo.faultTolerant)) {
      inputDF.option(ConstantUtils.STARTING_OFFSETS,
        RedisOffsetOperate.getLastCommittedOffsets(jobConfigVo.jobName
          , inputKafkaConfigVo.subscribeContent))
    }
    else {
      inputDF.option(ConstantUtils.STARTING_OFFSETS, "earliest")
      //        .option(ConstantUtils.ENDING_OFFSET, "latest")
    }
    if (null != inputKafkaConfigVo.maxOffsetsPerTrigger && 0 < inputKafkaConfigVo.maxOffsetsPerTrigger) {
      inputDF.option(ConstantUtils.MAX_OFFSETS_PER_TRIGGER, inputKafkaConfigVo.maxOffsetsPerTrigger)
    }
    inputDF
      .option(ConstantUtils.CHECKPOINT_LOCATION, PropertiesScalaUtils.getString("checkpointLocation"))
      .load()
  }

  //第二个步骤: 根据kafka数据schema和提取那些字段,并将不符合schema的源数据过滤出来
  def getKafkaJsonStructDataFrameBySchema(jobConfigVo: JobConfigVo, inputKafkaConfigVo: InputKafkaConfigVo,
                                          dataDF: DataFrame): (DataFrame, DataFrame) = {
    // kafka里面的JSON字符串[数据结构]转为spark的[数据结构]
    println(">>>>>>>>>>>>>>>>>inputKafkaConfigVo.dataSchema:" + inputKafkaConfigVo.dataSchema)
    val kafkaDataStructSchema = SchemaUtils.str2schema(inputKafkaConfigVo.dataSchema)
    println(">>>>>>>>>>>>>>>>>kafkaDataStructSchema:" + kafkaDataStructSchema)
    if (ConstantUtils.FAULT_TOLERANT_SNAPSHOT.equalsIgnoreCase(jobConfigVo.faultTolerant)) {
      //      val tempDF1 = dataDF.withWatermark("timestamp","10 seconds")
      //        .groupBy(window(col("timestamp"),"10 seconds","10 seconds"),col("timestamp"))
      val tempDF = dataDF.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), kafkaDataStructSchema, ConstantUtils.JSON_OPTIONS)
          .as("kafkaDataTempTable"), col("value").as("value_bak"))
      val errorQuery = tempDF.filter(col("kafkaDataTempTable").isNull).select("value_bak")
      val rightQuery = tempDF.filter(col("kafkaDataTempTable").isNotNull).select(col("kafkaDataTempTable.*"))
//        .withWatermark("timestamp", "60 seconds")
      (errorQuery, rightQuery)
    }

    else {
      val tempDF = dataDF
        .selectExpr("CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(offset AS LONG)", "CAST(value AS STRING)")
        .select(from_json(col("value"), kafkaDataStructSchema, ConstantUtils.JSON_OPTIONS)
          .as("kafkaDataTempTable"), col("topic"), col("partition"), col("offset"), col("value").as("value_bak"))
      val errorQuery = tempDF.filter(col("kafkaDataTempTable").isNull).select(col("value_bak"))
      val rightQuery = tempDF.select(col("kafkaDataTempTable.*"), col("topic"), col("partition"), col("offset"))
      (errorQuery, rightQuery)
    }
  }

  //第二个步骤: 根据kafka数据schema和提取那些字段
  def getKafkaTextStructDataFrameBySchema(jobConfigVo: JobConfigVo, inputKafkaConfigVo: InputKafkaConfigVo,
                                          dataDF: DataFrame): DataFrame = {
    // kafka里面的JSON字符串[数据结构]转为spark的[数据结构]
    //    val kafkaDataStructSchema = SchemaUtils.str2schema(inputKafkaConfigVo.dataSchema)
    val colNameArr = inputKafkaConfigVo.dataSchema.split(inputKafkaConfigVo.textSplit)
    if (ConstantUtils.FAULT_TOLERANT_SNAPSHOT.equalsIgnoreCase(jobConfigVo.faultTolerant)) {
      var tmpDF = dataDF.
        selectExpr("CAST(value AS STRING) as value")
        .withColumn("splitCols", functions.split(col("value"), inputKafkaConfigVo.textSplit))
      //给每子列赋名称
      colNameArr.zipWithIndex.foreach(x => {
        tmpDF = tmpDF.withColumn(x._1, col("splitCols").getItem(x._2))
      })
      tmpDF.drop("value").drop("splitCols")
    }
    else {
      var tmpDF = dataDF
        .selectExpr("CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(offset AS LONG)", "CAST(value AS STRING)")
        .withColumn("splitCols", functions.split(col("value"), inputKafkaConfigVo.textSplit))
      //给每子列赋名称
      colNameArr.zipWithIndex.foreach(x => {
        tmpDF = tmpDF.withColumn(x._1, col("splitCols").getItem(x._2))
      })
      tmpDF.drop("value").drop("splitCols")
    }
  }

  // 第三个步骤:执行层，采用spark-sql临时表进行查询
  def exeSqlDataFrame(jobConfigVo: JobConfigVo, spark: SparkSession, exeSqlConfigVo: ExeSqlConfigVo, kafkaStreamOutPutDF: DataFrame, staticData: mutable.HashMap[String, StaticInputSourceConfigVo]): (DataFrame, String, util.List[String]) = {
    //TODO 加多一步将静态数据加载进去，实现链表查询
    val staticTableList = StaticInputSource.loadAndRegister(spark, staticData)
    val targetTableName = StringUtils.getKafkaTmpTableName(exeSqlConfigVo.sqlContent, staticTableList)
    kafkaStreamOutPutDF
      .createOrReplaceGlobalTempView(targetTableName)
    val tmpTblDF = if (ConstantUtils.FAULT_TOLERANT_SNAPSHOT.equalsIgnoreCase(jobConfigVo.faultTolerant))
      spark.sql(StringUtils.appendGlobalTempDbName(exeSqlConfigVo.sqlContent))
    else
      spark.sql(StringUtils.appendTopicAndPartAndOffset(StringUtils.appendGlobalTempDbName(exeSqlConfigVo.sqlContent)))
    val udfList = exeSqlConfigVo.udfList
    val udfNames = new util.ArrayList[String]()
    var resultGlobalDf: DataFrame = tmpTblDF
    for (udfConfig: UDFConfigVo <- udfList) {
      val udfSqlColAlias = StringUtils.getSqlColAlias(exeSqlConfigVo.sqlContent,
        udfConfig.classMethod)
      udfNames.addAll(udfSqlColAlias)
      //UDF dealt
      resultGlobalDf = if ("1".equals(udfConfig.isNestCol)) {
        for (i <- 0 until udfSqlColAlias.size()) {
          val udfSqlColAlia = udfSqlColAlias.get(i)
          resultGlobalDf.select(
            from_json(col(udfSqlColAlia),
              SchemaUtils.str2schema(udfConfig.udfDataSchema),
              ConstantUtils.JSON_OPTIONS).as(udfSqlColAlias + "_add"),
            col("*")
          ).drop(udfSqlColAlia).withColumnRenamed(udfSqlColAlia + "_add", udfSqlColAlia)
        }
        resultGlobalDf
      } else if ("0".equals(udfConfig.isNestCol)) {
        for (i <- 0 until udfSqlColAlias.size()) {
          val udfSqlColAlia = udfSqlColAlias.get(i)
          resultGlobalDf.withColumn(udfSqlColAlia, from_json(col(udfSqlColAlia),
            SchemaUtils.str2schema(udfConfig.udfDataSchema),
            ConstantUtils.JSON_OPTIONS)).as(udfSqlColAlia + "_add").select(col("*")
            , col(udfSqlColAlias + "_add.*")).drop(udfSqlColAlia)
        }
        resultGlobalDf
      }
      else {
        resultGlobalDf
      }
    }
    //TODO 加入其他配置的静态数据流注册进去同一个Application
    spark.catalog.dropGlobalTempView(targetTableName)
    (resultGlobalDf, exeSqlConfigVo.dataPrimary, udfNames)
  }

  // 第四个步骤: 输出到外围系统，容错有快照实现
  def kafkaOutPutToEsBySnapshot(finalDF: DataFrame, jobConfigVo: JobConfigVo, outputEsConfigVo: OutputEsConfigVo,
                                checkpointLocation: String, dataPrimary: String) = {
    finalDF.drop("consumer_mark").writeStream
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, checkpointLocation)
      .format(ConstantUtils.ORG_ELASTICSEARCH_SPARK_SQL)
      .option(ConstantUtils.ES_NET_HTTP_AUTH_USER, outputEsConfigVo.esUser)
      .option(ConstantUtils.ES_NET_HTTP_AUTH_PASS, outputEsConfigVo.esPass)
      .option(ConstantUtils.ES_NODES, outputEsConfigVo.esNodes)
      .option(ConstantUtils.ES_PORT, outputEsConfigVo.esPort)
      .option(ConstantUtils.ES_RESOURCE, outputEsConfigVo.esResource)
      .option(ConstantUtils.ES_MAPPING_ID, dataPrimary)
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }

  // 第四个步骤: 输出到外围系统，容错有任务保存Offset到HBase
  def kafkaOutPutToEsByHbase(finalDF: DataFrame, jobConfigVo: JobConfigVo, outputEsConfigVo: OutputEsConfigVo,
                             checkpointLocation: String, dataPrimary: String) = {
    val esCfgMap: Map[String, String] = Map[String, String](
      ConstantUtils.ES_MAPPING_ID -> dataPrimary,
      ConstantUtils.ES_NET_HTTP_AUTH_USER -> outputEsConfigVo.esUser,
      ConstantUtils.ES_NET_HTTP_AUTH_PASS -> outputEsConfigVo.esPass,
      ConstantUtils.ES_NODES -> outputEsConfigVo.esNodes,
      ConstantUtils.ES_PORT -> outputEsConfigVo.esPort,
      ConstantUtils.ES_RESOURCE -> outputEsConfigVo.esResource,
      //          "es.nodes.wan.only"->"true",
      ConstantUtils.PUSH_DOWN -> "true")

    finalDF
      .drop("consumer_mark")
      .writeStream
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, checkpointLocation)
      .foreachBatch { (ds: Dataset[Row], _: Long) => {
        // 1.save data
        val offsetDF = ds.select(col("topic"), col("partition"), col("offset"))
        val dataDF = ds.drop("topic", "partition", "offset")
        EsSparkSQL.saveToEs(dataDF, esCfgMap)
        // 2.save offset
        //提取offset业务非相关数据FAULT_TOLERANT_HBASE
        OffsetOperate.MultSaveOffsets(jobConfigVo.jobName, offsetDF, jobConfigVo.version)
      }
      }
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }

  //TODO 第四个步骤: 输出到外围系统，容错有任务保存Offset到Redis
  def kafkaOutPutToEsByRedis(finalDF: DataFrame, jobConfigVo: JobConfigVo, outputEsConfigVo: OutputEsConfigVo,
                             checkpointLocation: String, dataPrimary: String) = {
    val esCfgMap: Map[String, String] = Map[String, String](
      ConstantUtils.ES_MAPPING_ID -> dataPrimary,
      ConstantUtils.ES_NET_HTTP_AUTH_USER -> outputEsConfigVo.esUser,
      ConstantUtils.ES_NET_HTTP_AUTH_PASS -> outputEsConfigVo.esPass,
      ConstantUtils.ES_NODES -> outputEsConfigVo.esNodes,
      ConstantUtils.ES_PORT -> outputEsConfigVo.esPort,
      ConstantUtils.ES_RESOURCE -> outputEsConfigVo.esResource,
      ConstantUtils.PUSH_DOWN -> "true")

    finalDF
      .drop("consumer_mark")
      .writeStream
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, checkpointLocation)
      .foreachBatch {
        (ds: Dataset[Row], _: Long) => {
          // 1.save data
          val offsetDF = finalDF.select(col("topic"), col("partition"), col("offset"))
          val dataDF = ds.drop("topic", "partition", "offset")
          EsSparkSQL.saveToEs(dataDF, esCfgMap)
          // 2.save offset
          //提取offset业务非相关数据FAULT_TOLERANT_REDIS
          RedisOffsetOperate.MultSaveOffsets(jobConfigVo.jobName, offsetDF)
        }
      }
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }

  // 总流程控制开关
  def kafkaStreamDealt(jobConfigVo: JobConfigVo, inputKafkaConfigVo: InputKafkaConfigVo,
                       executorConfigVoTuple: (String, ExecutorConfigVo),
                       outputSourceConfigVoMap: mutable.HashMap[String, OutputSourceConfigVo],
                       staticInputSourceConfigVoMap: mutable.HashMap[String, StaticInputSourceConfigVo],
                       spark: SparkSession,
                       processConf: ProcessStateVo): Unit = {

    //容错HDFS目录
    val checkpointLocation = CommonStreamUtils.getCheckpointLocation(jobConfigVo)
    // 第一步获取输入源数据
    val inputData = KafkaStreamUtils.getKafkaInputDataFrame(jobConfigVo, inputKafkaConfigVo, spark)
    // 第二把初始输入源数据转为结构化数据
    // 第三倍步逻辑处理层，对输入源数据sql或者其他逻辑处理返回kafka里面的数据
    val (finalDF: DataFrame, dataPrimary: String, udfNames: util.List[String]) = inputKafkaConfigVo.dataType match {
      case "JSON" | "json" | "Json" => { //1.如果kafka记录数据类型为json
        // 第二把初始输入JSON源数据转为结构化数据
        val kafkaStreamOutPutDF = getKafkaJsonStructDataFrameBySchema(jobConfigVo, inputKafkaConfigVo, inputData)
        //追加一步将异常数据写出
        val sourTopic = inputKafkaConfigVo.subscribeContent
        SourceDataUtils.errorDataSetOutPut(jobConfigVo, kafkaStreamOutPutDF._1, processConf, "INPUT_SCHEMA_MISMATCH", sourTopic)
        // 第三倍步逻辑处理层
        executorConfigVoTuple._1 match {
          case ConstantUtils.EXECUTE_SQL => {
            val exeSqlConfigVo = executorConfigVoTuple._2.asInstanceOf[ExeSqlConfigVo]
            exeSqlDataFrame(jobConfigVo, spark, exeSqlConfigVo, kafkaStreamOutPutDF._2, staticInputSourceConfigVoMap)
          }
          case _ => null
        }
      }
      case "TEXT" | "text" | "Text" => { //2.如果kafka记录数据类型为text
        // 第二把初始输入TEXT源数据转为结构化数据
        val kafkaStreamOutPutDF = getKafkaTextStructDataFrameBySchema(jobConfigVo, inputKafkaConfigVo, inputData)
        // 第三步逻辑处理层
        executorConfigVoTuple._1 match {
          case ConstantUtils.EXECUTE_SQL => {
            val exeSqlConfigVo = executorConfigVoTuple._2.asInstanceOf[ExeSqlConfigVo]
            exeSqlDataFrame(jobConfigVo, spark, exeSqlConfigVo, kafkaStreamOutPutDF, staticInputSourceConfigVoMap)
          }
          case _ => null
        }
      }
      case _ => { //3.如果kafka记录数据类型为其他
        null
      }
    }
    if (null == finalDF) {
      System.err.println("Usage: 不支持子此类型");
      System.exit(-1)
    }
    //第四步把逻辑处理好的数据存放外围系统
    outputSourceConfigVoMap.map { kv: (String, OutputSourceConfigVo) => {
      kv._1 match {
        case ConstantUtils.RESOURCE_ES => {
          // 输出源为ElasticSearch
          val outputEsConfigVo: OutputEsConfigVo = kv._2.asInstanceOf[OutputEsConfigVo]
          jobConfigVo.faultTolerant match {
            case ConstantUtils.FAULT_TOLERANT_SNAPSHOT =>
              kafkaOutPutToEsBySnapshot(finalDF, jobConfigVo, outputEsConfigVo, checkpointLocation, dataPrimary)
            case ConstantUtils.FAULT_TOLERANT_HBASE =>
              kafkaOutPutToEsByHbase(finalDF, jobConfigVo, outputEsConfigVo, checkpointLocation, dataPrimary)
            case _ => ConstantUtils.FAULT_TOLERANT_REDIS
              kafkaOutPutToEsByRedis(finalDF, jobConfigVo, outputEsConfigVo, checkpointLocation, dataPrimary)
            case _ => None
          }
        }
        case ConstantUtils.RESOURCE_JDBC => {
          // 输出源为JDBC
          val outputJdbcConfigVo: OutputJdbcConfigVo = kv._2.asInstanceOf[OutputJdbcConfigVo]
          val columnDataTypes = finalDF.schema.fields.map(_.dataType)
          KafkaToMysql.kafkaOutPutToMysql(finalDF, jobConfigVo, outputJdbcConfigVo, checkpointLocation, jobConfigVo.faultTolerant, "id", processConf)

          /*   val checkDataFrame = SourceDataUtils.ackColumCount(outputJdbcConfigVo, columnDataTypes)
             if (checkDataFrame._1 != null) {
               SourceDataUtils.errorDataTypeOutPut(jobConfigVo, finalDF, processConf, checkDataFrame._1, inputKafkaConfigVo.subscribeContent, udfNames)
             }
             else {
               KafkaToMysql.kafkaOutPutToMysql(finalDF, jobConfigVo, outputJdbcConfigVo, checkpointLocation, jobConfigVo.faultTolerant, checkDataFrame._2, processConf)
             }*/
        }
        case ConstantUtils.RESOURCE_FILE => {
          // 输出源为FILE
          val outputFileConfigVo: OutputFileConfigVo = kv._2.asInstanceOf[OutputFileConfigVo]
          KafkaToFile.kafkaOutPutToFile(finalDF, jobConfigVo, outputFileConfigVo, checkpointLocation, jobConfigVo.faultTolerant, dataPrimary)
        }
        case ConstantUtils.RESOURCE_KAFKA => {
          val outputKafkaConfigVo: OutputKafkaConfigVo = kv._2.asInstanceOf[OutputKafkaConfigVo]
          KafkaToKafka.kafkaOutPutToKafka(finalDF, jobConfigVo, outputKafkaConfigVo, checkpointLocation, jobConfigVo.faultTolerant, dataPrimary)
        }
        case _ => {
          None
        }
      }
    }
    }
  }
}
