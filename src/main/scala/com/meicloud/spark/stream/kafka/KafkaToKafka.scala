package com.meicloud.spark.stream.kafka

import java.util.Properties

import com.meicloud.spark.entity.CaseVo.{JobConfigVo, OutputKafkaConfigVo}
import com.meicloud.spark.utils.{ConstantUtils, PropertiesScalaUtils, TriggerUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame

object KafkaToKafka {
  def kafkaOutPutToKafka(finalDF: DataFrame, jobConfigVo: JobConfigVo, outputKafkaConfigVo: OutputKafkaConfigVo, checkpointLocation: String, faultTolerant: String, dataPrimary: String):Unit= {
    finalDF
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode(jobConfigVo.outputMode)
      .option(ConstantUtils.CHECKPOINT_LOCATION, checkpointLocation)
      .option(ConstantUtils.KAFKA_TOPIC,outputKafkaConfigVo.kafkaTopic)
      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER,outputKafkaConfigVo.kafkaBootstrapServers)
      .format(outputKafkaConfigVo.kafkaFormat)
      .trigger(TriggerUtils.getTrigger(jobConfigVo.frequency))
      .start()
      .awaitTermination()
  }


  def sendMsg(msg:String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", PropertiesScalaUtils.getString("kafka_broker"))
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
//    val topic = PropertiesScalaUtils.getString("error_msg_kafka_topic")
//    producer.send(new ProducerRecord(topic,msg))
  }
}
