package com.meicloud.spark.stream.kafka

import com.meicloud.spark.utils.{ConstantUtils, MySQLUtils, TriggerUtils}
import com.meicloud.spark.entity.CaseVo.{JobConfigVo, OutputJdbcConfigVo, ProcessStateVo}
import com.meicloud.spark.offset.hbase.OffsetOperate
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Created by yesk on 2019-3-20. 
  */
object KafkaToMysql {
  // 第四个步骤: 输出到外围系统，容错有任务保存Offset到HBase
  def kafkaOutPutToMysql(finalDF: DataFrame, jobConfigVo: JobConfigVo, outputJdbcConfigVo: OutputJdbcConfigVo,
                         checkpointLocation: String, faultTolerant: String, dataPrimary: String, processConf: ProcessStateVo) = {
     finalDF
      .writeStream
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, checkpointLocation)
      .foreachBatch {
        (ds: Dataset[Row], _: Long) => {
          //提取offset业务非相关数据FAULT_TOLERANT_HBASE
          faultTolerant match {
            case ConstantUtils.FAULT_TOLERANT_HBASE => {
              // 1.save data
              val offsetDF = ds.select(col("topic"), col("partition"), col("offset"))
              val dataDF = ds.drop("topic", "partition", "offset")
              MySQLUtils.insertOrUpdateDFtoDBUsePool(outputJdbcConfigVo, dataDF, dataPrimary.split("[,|，]"), jobConfigVo, processConf)
              // dataDF.write.mode(SaveMode.Overwrite ).jdbc(outputJdbcConfigVo.jdbcUrl,outputJdbcConfigVo.jdbcTable,prop)
              // 2.save offset
              OffsetOperate.MultSaveOffsets(jobConfigVo.jobName, offsetDF, jobConfigVo.version)
            }
            case ConstantUtils.FAULT_TOLERANT_SNAPSHOT => {
              ds.foreach(
                row => {
                  //TODO 测试
                  row.toString()
                }
              )
//              MySQLUtils.insertOrUpdateDFtoDBUsePool(outputJdbcConfigVo, ds, dataPrimary.split("[,|，]"), jobConfigVo, processConf)
//                          ds.write.mode(SaveMode.Overwrite ).jdbc(outputJdbcConfigVo.jdbcUrl,outputJdbcConfigVo.jdbcTable,prop)
            }
            //TODO REDIS
            case ConstantUtils.FAULT_TOLERANT_REDIS => {
              val vo = outputJdbcConfigVo
              MySQLUtils.insertOrUpdateDFtoDBUsePool(vo, ds, dataPrimary.split("[,|，]"), jobConfigVo, processConf)
              //            ds.write.mode(SaveMode.Overwrite ).jdbc(outputJdbcConfigVo.jdbcUrl,outputJdbcConfigVo.jdbcTable,prop)
            }
            case _ => {

            }
          }
        }
      }
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }

}
