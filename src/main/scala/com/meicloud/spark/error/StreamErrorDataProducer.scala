package com.meicloud.spark.error

import java.util.Properties

import com.google.gson
import com.google.gson.{JsonElement, JsonObject, JsonParser}
import com.meicloud.spark.utils.PropertiesScalaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
  *
  * 异常数据回收类
  */
object StreamErrorDataProducer {

  val props = new Properties()
  props.put("bootstrap.servers", PropertiesScalaUtils.getString("kafka_broker1"))
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  /**
    * 将结果数据将标志位consumer_mark加1后将数据发送出去,表示一次异常数据处理,然后将数据重新写入原来的topic中
    * @param record
    */
  def transformErrorDataAndSend(topic: String, record: String): Unit = {
    val arrValue = record.split("&")
    val gson = new JsonParser()
    val jsonObject = gson.parse(arrValue(3)).asInstanceOf[JsonObject]
    if (jsonObject.get("consumer_mark") == null) {
      jsonObject.addProperty("consumer_mark", "1")
    }
    else {
      var ts = jsonObject.get("consumer_mark").toString.toInt + 1
      jsonObject.addProperty("consumer_mark", ts.toString)
    }
    val rs = jsonObject.toString
    val targetTopic = topic.replaceAll("_error_data", "")
    val msg = new ProducerRecord[String, String](targetTopic, rs)
    try {
      producer.send(msg, new KafkaCallbackFunction())
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}


