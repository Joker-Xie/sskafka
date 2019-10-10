package com.meicloud.flink.stream;

import com.meicloud.flink.conntor.KafkaConnetor;
import com.meicloud.flink.shceme.utils.SchemaUtils;
import com.meicloud.flink.stream.utils.CommonStreamUtils;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import scala.Tuple2;
import scala.collection.mutable.HashMap;

public class KafkaStreamUtils {

    public static void kafkaStreamDealt(JobConfigVo jobConfigVo, InputKafkaConfigVo inputKafkaConfigVo, Tuple2<String, ExecutorConfigVo> executorConfigVoTuple, HashMap<String, OutputSourceConfigVo> outputSourceConfigVoMap, HashMap<String, StaticInputSourceConfigVo> staticInputSourceConfigVoMap, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        //容错HDFS目录
        String checkpointLocation = CommonStreamUtils.getCheckpointLocation(jobConfigVo);
        // 第一步获取输入源数据
        Table inputData = KafkaStreamUtils.getKafkaInputDataFrame(jobConfigVo, inputKafkaConfigVo, flink);
        // 第二把初始输入源数据转为结构化数据
        // 第三倍步逻辑处理层，对输入源数据sql或者其他逻辑处理返回kafka里面的数据
       /* val(finalDF:DataFrame, dataPrimary:String, udfNames:util.List[String]) =inputKafkaConfigVo.dataType match {
            case "JSON" | "json" | "Json" =>{ //1.如果kafka记录数据类型为json
                // 第二把初始输入JSON源数据转为结构化数据
                val kafkaStreamOutPutDF = getKafkaJsonStructDataFrameBySchema(jobConfigVo, inputKafkaConfigVo, inputData)
                //追加一步将异常数据写出
                val sourTopic = inputKafkaConfigVo.subscribeContent
                SourceDataUtils.errorDataSetOutPut(jobConfigVo, kafkaStreamOutPutDF._1, processConf, "INPUT_SCHEMA_MISMATCH", sourTopic)
                // 第三倍步逻辑处理层
                executorConfigVoTuple._1 match {
                    case ConstantUtils.EXECUTE_SQL =>{
                        val exeSqlConfigVo = executorConfigVoTuple._2.asInstanceOf[ExeSqlConfigVo]
                        exeSqlDataFrame(jobConfigVo, spark, exeSqlConfigVo, kafkaStreamOutPutDF._2, staticInputSourceConfigVoMap)
                    }
                    case _ =>null
                }
            }
            case "TEXT" | "text" | "Text" =>{ //2.如果kafka记录数据类型为text
                // 第二把初始输入TEXT源数据转为结构化数据
                val kafkaStreamOutPutDF = getKafkaTextStructDataFrameBySchema(jobConfigVo, inputKafkaConfigVo, inputData)
                // 第三步逻辑处理层
                executorConfigVoTuple._1 match {
                    case ConstantUtils.EXECUTE_SQL =>{
                        val exeSqlConfigVo = executorConfigVoTuple._2.asInstanceOf[ExeSqlConfigVo]
                        exeSqlDataFrame(jobConfigVo, spark, exeSqlConfigVo, kafkaStreamOutPutDF, staticInputSourceConfigVoMap)
                    }
                    case _ =>null
                }
            }
            case _ =>{ //3.如果kafka记录数据类型为其他
                null
            }
        }
        if (null == finalDF) {
            System.err.println("Usage: 不支持子此类型");
            System.exit(-1)
        }
        //第四步把逻辑处理好的数据存放外围系统
        outputSourceConfigVoMap.map {
            kv:
            (String, OutputSourceConfigVo) =>{
                kv._1 match {
                    case ConstantUtils.RESOURCE_ES =>{
                        // 输出源为ElasticSearch
                        val outputEsConfigVo:OutputEsConfigVo = kv._2.asInstanceOf[OutputEsConfigVo]
                        jobConfigVo.faultTolerant match {
                            case ConstantUtils.FAULT_TOLERANT_SNAPSHOT =>
                                kafkaOutPutToEsBySnapshot(finalDF, jobConfigVo, outputEsConfigVo, checkpointLocation, dataPrimary)
                            case ConstantUtils.FAULT_TOLERANT_HBASE =>
                                kafkaOutPutToEsByHbase(finalDF, jobConfigVo, outputEsConfigVo, checkpointLocation, dataPrimary)
                            case _ =>ConstantUtils.FAULT_TOLERANT_REDIS
                                kafkaOutPutToEsByRedis(finalDF, jobConfigVo, outputEsConfigVo, checkpointLocation, dataPrimary)
                            case _ =>None
                        }
                    }
                    case ConstantUtils.RESOURCE_JDBC =>{
                        // 输出源为JDBC
                        val outputJdbcConfigVo:OutputJdbcConfigVo = kv._2.asInstanceOf[OutputJdbcConfigVo]
                        val columnDataTypes = finalDF.schema.fields.map(_.dataType)
                        KafkaToMysql.kafkaOutPutToMysql(finalDF, jobConfigVo, outputJdbcConfigVo, checkpointLocation, jobConfigVo.faultTolerant, "id", processConf)

          *//*   val checkDataFrame = SourceDataUtils.ackColumCount(outputJdbcConfigVo, columnDataTypes)
             if (checkDataFrame._1 != null) {
               SourceDataUtils.errorDataTypeOutPut(jobConfigVo, finalDF, processConf, checkDataFrame._1, inputKafkaConfigVo.subscribeContent, udfNames)
             }
             else {
               KafkaToMysql.kafkaOutPutToMysql(finalDF, jobConfigVo, outputJdbcConfigVo, checkpointLocation, jobConfigVo.faultTolerant, checkDataFrame._2, processConf)
             }*//*
                    }
                    case ConstantUtils.RESOURCE_FILE =>{
                        // 输出源为FILE
                        val outputFileConfigVo:OutputFileConfigVo = kv._2.asInstanceOf[OutputFileConfigVo]
                        KafkaToFile.kafkaOutPutToFile(finalDF, jobConfigVo, outputFileConfigVo, checkpointLocation, jobConfigVo.faultTolerant, dataPrimary)
                    }
                    case ConstantUtils.RESOURCE_KAFKA =>{
                        val outputKafkaConfigVo:OutputKafkaConfigVo = kv._2.asInstanceOf[OutputKafkaConfigVo]
                        KafkaToKafka.kafkaOutPutToKafka(finalDF, jobConfigVo, outputKafkaConfigVo, checkpointLocation, jobConfigVo.faultTolerant, dataPrimary)
                    }
                    case _ =>{
                        None
                    }*/
    }

    private static Table getKafkaInputDataFrame(JobConfigVo jobConfigVo, InputKafkaConfigVo inputKafkaConfigVo, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        StreamTableEnvironment tableEnvironment = flink._2;
        StreamTableDescriptor tmpDescriptor =  tableEnvironment.connect(KafkaConnetor.getConnetor(inputKafkaConfigVo));
        String dataType = inputKafkaConfigVo.dataType().toLowerCase();

        switch (dataType){
            case "json" :
                //TODO 源数据获取。
                tmpDescriptor.withFormat(new Json().schema(SchemaUtils.getJsonSchema(inputKafkaConfigVo)));
                break;
            case "csv" :
                break;

        }
        return null;
    }
}
