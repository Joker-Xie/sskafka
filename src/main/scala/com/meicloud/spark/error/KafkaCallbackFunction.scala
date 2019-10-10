package com.meicloud.spark.error

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
/**
 * 
 * kafka callback 函数用于返回的producer 发送数据出错返回原因
 */
class KafkaCallbackFunction extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      exception.printStackTrace()
    }
  }
}