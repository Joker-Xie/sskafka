package com.meicloud.spark.offset.hbase


import com.meicloud.spark.utils.{PropertiesScalaUtils, ZkUtils}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection._


/**
  * 手工操作offset
  *        1 从hbase获取offset，从kafka拉取数据
  *        2 数据处理完后，把until offset 保存到hbase
  *        3 kafka 长时间挂掉之后，从kafka最早的offset 开始读取 此处还需要处理
  *
  */
object OffsetOperate  {
  case class OffsetMang(topicName: String, partition: Int, offset: Long)
  def main(args: Array[String]): Unit = {
//    saveOffsets("kafka2es-car-003","test.spark.stream.kafka.11",0,1,1)
val spark = SparkSession
  .builder
  .master("local[16]")
  .appName("Test")
  .getOrCreate()

    import spark.implicits._

////    import spark.sqlContext.implicits._
//    val df = spark.sparkContext.textFile("file:///D:/myoffice/2019project/sskafka/src/main/resources/offset.txt")
//      .map(_.split(","))
//        .map(arr=>
//          OffsetMang(arr(0),java.lang.Integer.parseInt(arr(1)),java.lang.Long.parseLong(arr(2))))
//      .toDF()

    println(getLastCommittedOffsets("kafka2es-car-003","test.spark.stream.kafka.11",0))

  }
  /*
 2 Save offsets for each batch into HBase
 3*/
  def MultSaveOffsets(CONFIG_ID:String,df:DataFrame,version:Long=0)={
    val hbaseTableName = PropertiesScalaUtils.getString("hbaseTableName") ;
    val offsetsCf =PropertiesScalaUtils.getString("hbaseCf")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource("src/main/resources/hbase-site.xml")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    df
      .foreachPartition(itr=>{
      val hbaseConf = HBaseConfiguration.create()
      val conn = ConnectionFactory.createConnection(hbaseConf)
      hbaseConf.addResource("src/main/resources/hbase-site.xml")

      val table = new HTable(hbaseConf,TableName.valueOf(hbaseTableName))
      table.setAutoFlush(false,false)
      itr.foreach(x=>{
        val rowKey = CONFIG_ID+":" +x.getString(0) + ":" + version
        val put = new Put(rowKey.getBytes)
        put.addColumn(Bytes.toBytes(offsetsCf),Bytes.toBytes(x.getInt(1).toString),
          Bytes.toBytes(x.getLong(2).toString))
          table.put(put)
        table.flushCommits()
      })
      table.close()
    })
  }

  def saveOffsets(CONFIG_ID:String,TOPIC_NAME:String,PARTITION_ID:Int,OFFSET_ID:Int,
                  version:Long=0) ={
    val hbaseTableName = PropertiesScalaUtils.getString("hbaseTableName") ;
    val offsetsCf =PropertiesScalaUtils.getString("hbaseCf")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource("src/main/resources/hbase-site.xml")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val rowKey = CONFIG_ID+":" +TOPIC_NAME + ":" + version
    val put = new Put(rowKey.getBytes)
     put.addColumn(Bytes.toBytes(offsetsCf),Bytes.toBytes(PARTITION_ID.toString),
            Bytes.toBytes(OFFSET_ID.toString))
    table.put(put)
    conn.close()
  }

  /* Returns last committed offsets for all the partitions of a given topic from HBase in
    following  cases.
  */
  def getLastCommittedOffsets(CONFIG_ID:String,TOPIC_CONTENT:String,version:Int):String ={
    val hbaseTableName = PropertiesScalaUtils.getString("hbaseTableName") ;
    val offsetsCf =PropertiesScalaUtils.getString("hbaseCf")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource("src/main/resources/hbase-site.xml")


    val topicArr = TOPIC_CONTENT.split(",")
    val startOffset = topicArr.map{TOPIC_NAME=>
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf(hbaseTableName))
      val preRowKey = CONFIG_ID + ":" + TOPIC_NAME + ":" + version
      val scan = new Scan()
      val filter = new PrefixFilter(Bytes.toBytes(preRowKey))
      scan.setFilter(filter).setReversed(true)
      val scanner = table.getScanner(scan)

      val result = scanner.next()
      var hbaseNumberOfPartitionsForTopic = 0 //Set the number of partitions discovered for a topic in HBase to 0
      if (result != null) {
        //If the result from hbase scanner is not null, set number of partitions from hbase to the  number of cells
        hbaseNumberOfPartitionsForTopic = result.listCells().size()
      }
      val allPartitions = Range(0,ZkUtils.getPartitionsForTopics(TOPIC_NAME)).toSet[Int]

      if (hbaseNumberOfPartitionsForTopic == 0) {
        // initialize fromOffsets to beginning
        scanner.close()
        conn.close()

        val allPartitionStr = allPartitions.map(qualifier=>"\"" +qualifier + "\":" + "-2").mkString(",")

        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>")
        println(TOPIC_NAME+":{"+allPartitionStr+"}")
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>")

        "\""+TOPIC_NAME+"\":{"+allPartitionStr+"}"


      } else {
        val topicAndPartitionOffsetArr = new Array[String](2)
        topicAndPartitionOffsetArr(0) = "\"" + TOPIC_NAME + "\""
        val partitionAndOffsetArr = new Array[String](hbaseNumberOfPartitionsForTopic)
        val row = result.raw()
        var idx=0
        var hbPartition = Set[Int]()
        for (kv <-  row) {
          val qualifier =  Bytes.toString(kv.getQualifier())
//          println(">>>>>>>>:Integer.parseInt(qualifier)"+Integer.parseInt(qualifier))
          hbPartition = hbPartition.+(Integer.parseInt(qualifier))
//          allPartitions = allPartitions.- Integer.parseInt(Bytes.toString(kv.getQualifier()))
//          if (idx==0)hbBuilder.append(qualifier) else hbBuilder.append(","+qualifier)
          val fromOffset = Integer.parseInt(Bytes.toString(kv.getValue()))+1
            partitionAndOffsetArr(idx) = "\"" +qualifier + "\":" + fromOffset
          idx=idx+1
        }
        val otherP = allPartitions -- hbPartition
        var subPartition = ""
        if (otherP.size >0)  subPartition = ","+otherP.map(qualifier=>"\"" +qualifier + "\":" + "-2").mkString(",")
        topicAndPartitionOffsetArr(1) = "{"+partitionAndOffsetArr.mkString(",")+subPartition+"}"

        scanner.close()
        conn.close()
        topicAndPartitionOffsetArr.mkString("", ":", "")
      }
    }.mkString("{", ",", "}")

//    conn.close()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(startOffset)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>")
    startOffset
  }
}

