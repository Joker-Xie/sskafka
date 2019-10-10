package com.meicloud.spark.entity

import java.util.Properties

import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yesk on 2019-1-28. 
  */
object CaseVo {

  abstract class InputSourceConfigVo(sourceType: String) extends Serializable

  abstract class OutputSourceConfigVo(sourceType: String) extends Serializable

  abstract class ExecutorConfigVo(executorType: String) extends Serializable

  abstract class StaticInputSourceConfigVo(sourceType: String) extends Serializable

  case class JobConfigVo(
                          jobId: String,
                          jobName: String,
                          version: Int,
                          tradeName: String,
                          updateTime: String,
                          frequency: Int,
                          outputMode: String,
                          faultTolerant: String,
                          master: String
                        )

  case class InputKafkaConfigVo(
                                 sourceId: String,
                                 sourceType: String,
                                 kafkaBootstrapServers: String,
                                 subscribeType: String,
                                 subscribeContent: String,
                                 startingOffsets: String,
                                 endingOffsets: String,
                                 maxOffsetsPerTrigger: Int,
                                 dataType: String,
                                 dataSchema: String,
                                 textSplit: String
                               ) extends InputSourceConfigVo(sourceType)

  case class OutputEsConfigVo(
                               sourceId: String,
                               sourceType: String,
                               esNodes: String,
                               esPort: String,
                               esResource: String,
                               esUser: String,
                               esPass: String
                             ) extends OutputSourceConfigVo(sourceType)

  case class ExeSqlConfigVo(executorId: String,
                            executorType: String,
                            sqlContent: String,
                            dataPrimary: String,
                            udfList: ArrayBuffer[UDFConfigVo]
                           ) extends ExecutorConfigVo(executorType)

  case class UDFConfigVo(jarPath: String,
                         classPath: String,
                         classMethod: String,
                         isNestCol: String,
                         udfDataSchema: String
                        )

  case class StreamKafkaToESVo(
                                jobConfigVo: JobConfigVo,
                                kafkaConfigVo: InputKafkaConfigVo,
                                exeSqlConfigVo: ExeSqlConfigVo,
                                outputSourceConfigVo: ArrayBuffer[OutputSourceConfigVo]
                              )

  //
  case class Kafka2ESConfigVo(configId: String,
                              tradeName: String,
                              kafkaBootstrapServers: String,
                              subscribeType: String,
                              subscribeContent: String,
                              startingOffsets: String,
                              endingOffsets: String,
                              maxOffsetsPerTrigger: Int,
                              dataType: String,
                              dataSchema: String,
                              outputMode: String,
                              esNodes: String,
                              esPort: String,
                              esResource: String,
                              esUser: String,
                              esPass: String,
                              version: Int
                             )

  case class Kafka2KafkaConfigVo(configId: String, tradeName: String,
                                 kafkaBootstrapServers: String, subscribeType: String,
                                 subscribeContent: String, startingOffsets: String,
                                 endingOffsets: String, maxOffsetsPerTrigger: Int,
                                 dataType: String, dataSchema: String,
                                 outputMode: String,
                                 outKafkaBootstrapServers: String, outTopic: String,
                                 outTrigger: String
                                )

  case class Kafka2HbaseConfigVo(configId: String, tradeName: String,
                                 kafkaBootstrapServers: String, subscribeType: String,
                                 subscribeContent: String, startingOffsets: String,
                                 endingOffsets: String, maxOffsetsPerTrigger: Int,
                                 dataType: String, dataSchema: String,
                                 outputMode: String,
                                 outHbaseCatalog: String,
                                 outTrigger: String
                                )

  case class Kafka2JDBCConfigVo(configId: String, tradeName: String,
                                kafkaBootstrapServers: String, subscribeType: String,
                                subscribeContent: String, startingOffsets: String,
                                endingOffsets: String, maxOffsetsPerTrigger: Int,
                                dataType: String, dataSchema: String,
                                outputMode: String,
                                outJdbcUrl: String,
                                outJdbcDbtable: String,
                                outJdbcUser: String,
                                outJdbcPassword: String,
                                outTrigger: String
                               )


  case class UdfConfig(udfName: String, udfClassFullname: String)

  case class BatchJobConfig(udfConfig: List[UdfConfig],
                            sparkSql: String)

  case class OutputJdbcConfigVo(
                                 sourceId: String,
                                 sourceType: String,
                                 jdbcDriver: String,
                                 jdbcUrl: String,
                                 jdbcUser: String,
                                 jdbcPass: String,
                                 jdbcTable: String,
                                 minPoolSize: Int,
                                 maxPoolSize: Int,
                                 acquireIncrement: Int,
                                 maxStatements: Int
                               ) extends OutputSourceConfigVo(sourceType)

  case class OutputFileConfigVo(
                                 sourceId: String,
                                 sourceType: String,
                                 fileFormat: String,
                                 filePath: String
                               ) extends OutputSourceConfigVo(sourceType)

  case class OutputKafkaConfigVo(
                                  sourceId: String,
                                  sourceType: String,
                                  kafkaTopic: String,
                                  kafkaBootstrapServers: String,
                                  kafkaFormat: String
                                ) extends OutputSourceConfigVo(sourceType)

  case class ProcessStateVo(
                             errorNum: LongAccumulator,
                             totalNum: LongAccumulator,
                             finishNum: LongAccumulator
                           )

  case class KafkaStaticConfVo(
                                sourceId: String,
                                sourceType: String,
                                kafkaBootstrapServers: String,
                                subscribeType: String,
                                tableName: String,
                                subscribeContent: String,
                                startingOffsets: String,
                                endingOffsets: String,
                                maxOffsetsPerTrigger: Int,
                                dataType: String,
                                dataSchema: String,
                                textSplit: String
                              ) extends StaticInputSourceConfigVo(sourceType)

  case class JdbcStaticConfVo(
                               sourceId: String,
                               sourceType: String,
                               jdbcDriver: String,
                               jdbcUrl: String,
                               jdbcUser: String,
                               jdbcPass: String,
                               jdbcTableName: String,
                               registTableName:String,
                               minPoolSize: Int,
                               maxPoolSize: Int,
                               acquireIncrement: Int,
                               maxStatements: Int
                             ) extends StaticInputSourceConfigVo(sourceType)

  case class FileStaticConfVo(
                               sourceId: String,
                               sourceType: String,
                               fileFormat: String,
                               filePath: String,
                               schema: String,
                               tableName: String
                             ) extends StaticInputSourceConfigVo(sourceType)


  case class EsStaticConfVo(
                             sourceId: String,
                             sourceType: String,
                             esNodes: String,
                             esPort: String,
                             esResource: String,
                             tableName: String,
                             esUser: String,
                             esPass: String
                           ) extends StaticInputSourceConfigVo(sourceType)

}
