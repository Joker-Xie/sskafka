package com.meicloud.spark.error

import java.{lang, util}
import java.util.Properties

import com.meicloud.spark.offset.redis.RedisOffsetOperate
import com.meicloud.spark.utils.{JedisConnectionPool, PropertiesScalaUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

/**
  *
  * 异常数据消费类
  */
object StreamErrorDataConsumer {
  val props = new Properties()
  props.put("bootstrap.servers", PropertiesScalaUtils.getString("kafka_broker"))
  props.put("group.id", "kafka2kafka")
  props.put("enable.auto.commit", "false")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


  /**
    * 启动消费异常数据
    *
    * @param jobname
    * @param topic
    */
  def startConsumer(jobname: String, topic: String): Unit = {
    val consumer = new KafkaConsumer[String, String](props)
    val offsetObj = RedisOffsetOperate.getOffsetMsg2Kafka(jobname, topic, consumer)
    val conn = JedisConnectionPool.getConnections()
    val topics = new util.ArrayList[String]()
    topics.add(topic)
    val valueMap = new java.util.HashMap[String, String]()
    //设置分区信息
    consumer.assign(offsetObj._1)
    //获取结束的offsetMsg
    val topicEndOffset = consumer.endOffsets(offsetObj._1)
    //设置kafka各分区开始的消费offset
    val itor = offsetObj._2.keySet().iterator()
    while (itor.hasNext) {
      val obj = itor.next()
      consumer.seek(obj, offsetObj._2.get(obj))
    }
    //开始消费数据
    while (true) {
      val records = consumer.poll(100)
      val recordItor = records.iterator()
      while (recordItor.hasNext) {
        val record = recordItor.next()
        //数据转换,并使用kafka发送出去
        StreamErrorDataProducer.transformErrorDataAndSend(topic, record.value().toString)
        //判断分区是否消费到设置的结束的位置
        val partition = record.partition()
        val offset = record.offset()
        //将消费offset保存在redis中
        valueMap.put(partition.toString, offset.toString)
        val key = jobname + ":" + topic
        conn.hmset(key, valueMap)
        valueMap.clear()
        //判断数据是否消费到结束位置
        if (offset == (topicEndOffset.get(new TopicPartition(topic, partition)) - 1)) {
          val errorNum = getAbnormalDataNum(offsetObj._2, topicEndOffset)
          Regular4ErrorData.updateErroNum(jobname,errorNum)
          consumer.close()
          throw new Exception()
        }
      }
    }
  }

  /**
    * 用于计算异常数据再处理的数量
    *
    * @param topicStartOffset
    * @param topicEndOffset
    */
  def getAbnormalDataNum(topicStartOffset: util.HashMap[TopicPartition, Long], topicEndOffset: util.Map[TopicPartition, lang.Long]): Long = {
    val keys = topicEndOffset.keySet().iterator()
    var errorNum = 0L;
    while (keys.hasNext) {
      val k = keys.next()
      val start = topicStartOffset.get(k)
      val stop = topicEndOffset.get(k)
      errorNum += (stop - start)
    }
    errorNum
  }


  def main(args: Array[String]): Unit = {
    try {
      //      starConsumer("job_kafka2es_test_sample11", "gxt.comment.white.redo.test")
      startConsumer("job_kafka2es_test_sample", "gxt.test.white.redo.flow_test1")
    } catch {
      case e: Exception => println("Error data has been processed.")
    }
  }
}
