package com.meicloud.spark.utils

/**
  * Created by yesk on 2019-3-7. 
  */
object ConstantUtils {
  //流处理日志级别
  val LOG_LEVEL_ERROR = "ERROR"
  val LOG_LEVEL_WORN = "WORN"
  val LOG_LEVEL_INFO = "INFO"

  //输入源 --KAFKA --参数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  val FORMAT_KAFKA = "kafka"
  val KAFKA_BOOTSTRAP_SERVER = "kafka.bootstrap.servers"
  val STARTING_OFFSETS = "startingOffsets"
  val ENDING_OFFSET = "endingOffsets"
  val MAX_OFFSETS_PER_TRIGGER = "maxOffsetsPerTrigger"


  //输出源 --ES --参数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  val ES_NET_HTTP_AUTH_USER = "es.net.http.auth.user"
  val ES_NET_HTTP_AUTH_PASS = "es.net.http.auth.pass"
  val ES_NODES = "es.nodes"
  val ES_PORT = "es.port"
  val ES_RESOURCE = "es.resource"
  val ES_NODE_WAN_ONLY = "es.nodes.wan.only"
  val PUSH_DOWN = "pushdown"
  val ES_MAPPING_ID = "es.mapping.id"
  val ORG_ELASTICSEARCH_SPARK_SQL = "org.elasticsearch.spark.sql"

  //输出源  --Kafka--参数
  val KAFKA_TOPIC="topic"
  //流处理 hdfs快照目录
  val CHECKPOINT_LOCATION = "checkpointLocation"

  //节点类型
  val NODE_INPUT = "input"
  val NODE_EXECUTE = "execute"
  val NODE_OUTPUT = "output"
  val NODE_STATICINPUT = "staticinput"

  //资源类型
  val RESOURCE_KAFKA = "kafka"
  val RESOURCE_ES = "es"
  val RESOURCE_JDBC = "jdbc"
  val RESOURCE_FILE = "file"

  //执行引擎类型
  val EXECUTE_SQL = "sql"
  val EXECUTE_SCALA = "scala"
  val EXECUTE_PYTHON = "python"
  val EXECUTE_JAVA = "java"

  //dataframe json转换参数
  val NEST_TIMESTAMP_FORMAT  = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
  val JSON_OPTIONS  =  Map("timestampFormat" -> ConstantUtils.NEST_TIMESTAMP_FORMAT)

  //计算引擎
  val CALC_ENGINE_SPARK = "spark"
  val CALC_ENGINE_FLINK = "flink"

  //容错方式
  val FAULT_TOLERANT_SNAPSHOT = "snapshot"
  val FAULT_TOLERANT_HBASE = "hbase"
  val FAULT_TOLERANT_REDIS = "redis"


  //job parameter
  val TRIGGER_UNIT_SECOND = "second"
  val TRIGGER_UNIT_SECONDS = "seconds"











}
