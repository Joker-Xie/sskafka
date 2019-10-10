package com.meicloud.spark.stream.kafka

import com.meicloud.spark.entity.CaseVo.{JobConfigVo, OutputFileConfigVo}
import com.meicloud.spark.utils.{ConstantUtils, TriggerUtils}
import org.apache.spark.sql.DataFrame

/**
  * Created by yesk on 2019-3-20. 
  */
object KafkaToFile {
  // 第四个步骤: 输出到外围系统，容错有任务保存Offset到snapshat
  def kafkaOutPutToFile(finalDF: DataFrame, jobConfigVo: JobConfigVo, outputFileConfigVo: OutputFileConfigVo,
                        checkpointLocation: String, faultTolerant: String, dataPrimary: String) = {
    finalDF
      .writeStream
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, checkpointLocation)
      .format(outputFileConfigVo.fileFormat)
      .option("path", outputFileConfigVo.filePath)
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }
}
