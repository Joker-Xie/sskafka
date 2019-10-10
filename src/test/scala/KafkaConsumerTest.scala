import java.io.PrintWriter
import java.util
import java.util.{List, Map, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer


//import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

object KafkaConsumerTest {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
//    props.put("bootstrap.servers", "10.18.126.43:9092,10.18.126.44:9092,10.18.126.45:9092")
            props.put("bootstrap.servers", "10.16.25.176:9092,10.16.26.201:9092,10.16.26.202:9092")
    props.put("group.id", "test12")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    //    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer(props)
    val topics = new util.ArrayList[String]()
    //    val partitions = new util.ArrayList[TopicPartition]()
    topics.add("gxt.test.sdc.dengyh4_test_order")
    //    partitions.add(new TopicPartition("gxt.test.white.redo", 0))
    //    consumer.assign(partitions)
    //    consumer.seek(new TopicPartition("gxt.test.white.redo", 0), 0)
    consumer.subscribe(topics)
    var count = 0;
//    val writer = new PrintWriter("E:\\source.txt")
    while (true) {
      val records = consumer.poll(100)
      if (!records.isEmpty) {
        val iterable = records.iterator()
        while (iterable.hasNext) {
          val record = iterable.next()
//          writer.write(record.value().toString + "\n")
          count += 1
          println(count)
        }
      }
      if (count == 70000) {
//        writer.close()
        System.exit(1)
      }

    }
  }
}
