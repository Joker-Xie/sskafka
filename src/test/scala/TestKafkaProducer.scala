import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.Source


object TestKafkaProducer {

  def sendMsg2Kafka(): Unit = {
    val props = new Properties()

//    props.put("bootstrap.servers", "10.18.126.43:9092,10.18.126.44:9092,10.18.126.45:9092")
//    props.put("bootstrap.servers", "192.168.112.128:9092")
            props.put("bootstrap.servers", "10.16.25.176:9092,10.16.26.201:9092, 10.16.26.202:9092")
    //    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    //    val topic = "gxt.comment.white.redo.test_error_data"
//    val topic = "test"
        val topic = "gxt.test.sdc.dengyh4_test_order"
    //    for (i <- 1 to 10){
    //      producer.send(new ProducerRecord(topic, "key-" + i, "msg-" + i))
    //    }
    val source = Source.fromFile("D:\\MyData\\xiehy11\\Desktop\\order.txt").getLines()
    //    val source = Source.fromFile("D:\\MyData\\xiehy11\\Desktop\\测试异常数据.txt").getLines()
    var count = 0
    while (source.hasNext) {
//      val line = TestParserSql.createOrderInfo("random")
      val line = source.next()
      producer.send(new ProducerRecord(topic, line), new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            exception.printStackTrace()
          }
        }
      })
      Thread.sleep(1)
      count = count + 1
      println("send Msg" + count)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    sendMsg2Kafka()
  }
}
