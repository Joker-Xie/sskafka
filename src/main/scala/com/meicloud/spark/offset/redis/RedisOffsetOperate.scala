package com.meicloud.spark.offset.redis

import java.util

import com.meicloud.spark.utils.JedisConnectionPool
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.{DataFrame, Dataset, Row}


/**
  * 手工操作offset
  * 1 从redis获取offset，从kafka拉取数据
  * 2 数据处理完后，把until offset 保存到redis
  * 3 kafka 长时间挂掉之后，从kafka最早的offset 开始读取 此处还需要处理
  *
  */

object RedisOffsetOperate {
  def main(args: Array[String]): Unit = {
    //    val obj = getOffsetMsg2Kafka(",")
  }

  case class OffsetMsg(
                        offsetObj: Map[String, Map[String, Map[String, Long]]]
                      ) extends Serializable

  /**
    * 将offset 数据存在redis中
    *
    * @param CONFIG_ID
    * @param df
    */
  def MultSaveOffsets(CONFIG_ID: String, df: DataFrame) = {
    val obj = RedisOffsetObj
    df.writeStream
      .foreachBatch { (ds: Dataset[Row], _: Long) =>
        ds.foreachPartition {
          partitions => {
            val conn = JedisConnectionPool.getConnections()
            val valueMap = new java.util.HashMap[String, String]()
            partitions.foreach(
              record => {
                valueMap.put(record.getInt(1).toString, record.getLong(2).toString)
                val key = CONFIG_ID + ":" + record.getString(0)
                conn.hmset(key, valueMap)
                valueMap.clear()
              }
            )
            JedisConnectionPool.rebackPool(conn)
          }
        }
      }
      .start()
  }

  /**
    * 将存于redis的offset数据提取出来,
    * 并转成saprk struct 读取kafka的数据格式"""{xxx:{xxx:xxx}}"""
    *
    * @param CONFIG_ID
    * @param Topic
    */
  def getLastCommittedOffsets(CONFIG_ID: String, Topic: String): String = {
    val conn = JedisConnectionPool.getConnections()
    val key = CONFIG_ID + ":" + Topic
    if (conn.exists(key)) {
      val keys = key.split(":")
      val str = conn.hgetAll(key)
      val itr = str.entrySet().iterator()
      var rs = ""
      while (itr.hasNext) {
        val key = itr.next()
        rs = rs + "\"" + key.getKey + "\":" + (key.getValue + 1) + ","
      }
      "{\"" + keys(1) + "\":{" + rs.substring(0, rs.length - 1) + "}}"
    }
    else {
      "earliest"
    }
  }

  /**
    * 获取redis的kafka消费信息
    *
    * @param jobName
    * @param topic
    */
  def getOffsetMsg2Kafka(jobName: String, topic: String, consumer: KafkaConsumer[String, String]): (util.ArrayList[TopicPartition], util.HashMap[TopicPartition, Long]) = {
    val conn = JedisConnectionPool.getConnections()
    val key = jobName + ":" + topic
    val partitionsMsg = new util.ArrayList[TopicPartition]()
    val offsetMsg = new util.HashMap[TopicPartition, Long]()
    if (conn.exists(key)) {
      val str = conn.hgetAll(key)
      val itr = str.entrySet().iterator()
      while (itr.hasNext) {
        val valueKey = itr.next()
        partitionsMsg.add(new TopicPartition(key.split(":")(1), valueKey.getKey.toInt))
        offsetMsg.put(new TopicPartition(key.split(":")(1), valueKey.getKey.toInt), valueKey.getValue.toLong+1)
      }
      (partitionsMsg, offsetMsg)
    }
    else {
      val partitionList = consumer.partitionsFor(topic)
      for (i <- 0 until partitionList.size()) {
        partitionsMsg.add(new TopicPartition(topic, partitionList.get(i).partition()))
      }
      val initOffset = consumer.beginningOffsets(partitionsMsg)
            val itor = initOffset.keySet().iterator()
            while(itor.hasNext){
              val partition = itor.next()
              val starOffset = initOffset.get(partition)
              offsetMsg.put(partition, starOffset)
            }
      (partitionsMsg, offsetMsg)
    }
  }
}