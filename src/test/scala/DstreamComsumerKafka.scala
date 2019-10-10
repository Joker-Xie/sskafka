import java.sql.SQLException

import com.meicloud.spark.offset.redis.{RedisOffsetObj, RedisOffsetOperate}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StringType
//import com.meicloud.spark.udf.{UDFTest, UdfRegister}
import com.meicloud.spark.udf.UdfRegister
import com.meicloud.spark.utils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object DstreamComsumerKafka {
  val state: String = null
  val rank: Int = 0
  var lastState: LastState = null

  case class LastState(state: String, rank: Int)

  def getStrLen(str: String): Int = {
    str.length
  }

  def getRankNum(str: String): Int = {
    val strr = if (str == null || str == "") "" else str
    if (lastState == null) {
      lastState = LastState.apply(strr, 0)
    }
    else {
      val olderState = lastState.state
      if (olderState.equals(strr)) {
        val rankNum = lastState.rank + 1
        lastState = LastState.apply(strr, rankNum)
      }
      else {
        val rankNum = 0
        lastState = LastState.apply(strr, rankNum)
      }
    }
    lastState.rank
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("DstreamComsumerKafka")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()
    import ss.implicits._
    ss.sparkContext.setLogLevel("ERROR")
    //    val offset = RedisOffsetOperate.getLastCommittedOffsets("gxt.test.white.redo.flow_test1", "gxt.test.white.redo.flow_test1")
    //    println(offset)
    val inputDF = ss
//      .read.textFile("D:\\MyData\\xiehy11\\Desktop\\other.txt")
          .readStream
          .format(ConstantUtils.FORMAT_KAFKA)
          //      测试kafka集群
          //      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, "10.18.126.43:9092,10.18.126.44:9092,10.18.126.45:9092,10.18.126.46:9092,10.18.126.47:9092")
          .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, "10.16.25.176:9092,10.16.26.201:9092,10.16.26.202:9092")
          //测试topic
          .option("subscribe", "gxt.test.sdc.dengyh4_test_order")
          //      .option("subscribe", "gxt.test.white.redo.flow_test1")
          //      .option(ConstantUtils.CHECKPOINT_LOCATION, "/tmp/checkpoint/")
          .option(ConstantUtils.STARTING_OFFSETS, "earliest")
          //      .option(ConstantUtils.STARTING_OFFSETS, """{"gxt.comment.white.redo.test":{"2":23067429,"4":23067431,"1":23067431,"3":23067429,"0":23067430}}""")
          //      .option(ConstantUtils.STARTING_OFFSETS,"")
          .option(ConstantUtils.MAX_OFFSETS_PER_TRIGGER, "1000")
          .load()


    val str = "{\"item_parser_code\": \"string\",\"shop_name_eng\": \"string\",\"jan_code\": \"string\",\"parser_code\": \"string\",\"item_id\": \"string\",\"tax_price\": \"string\",\"shop_name\": \"string\",\"title\": \"string\",\"platform\": \"string\",\"fetch_time\": \"long\",\"store_kafka_topic\": \"string\",\"shop_id\": \"string\",\"item_url\": \"string\",\"platform_eng\": \"string\",\"store_redis_key\": \"string\",\"model\": \"string\",\"seed_unique\": \"string\"}"

    //
    val schema = SchemaUtils.str2schema(str)


    val outDF = inputDF
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema, ConstantUtils.JSON_OPTIONS)
        .as("kafkaDataTempTable"), col("value").as("value_bak"))
      .filter(col("kafkaDataTempTable").isNotNull)
      .select(col("kafkaDataTempTable.*"))


    outDF.createOrReplaceGlobalTempView("test")
    ss.udf.register("rank_num", getRankNum(_: String))
//    val tempDF = ss.sql(" select t.jan_code ,t.time,WINDOW FROM (select jan_code , to_timestamp(fetch_time) as time  FROM global_temp.test ) as t GROUP BY t.jan_code,t.time,WINDOW('t.time','10 seconds','5 seconds') ORDER BY t.jan_code,t.time DESC  ")
//    val tempDF = ss.sql("select * from (select t.jan_code,t.fetch_time,rank_num(t.jan_code) as num from (select jan_code,fetch_time,WINDOW from global_temp.test group by jan_code,fetch_time ,WINDOW(to_timestamp(fetch_time/1000),'10 seconds','10 seconds') ORDER BY jan_code, fetch_time desc ) as t )as tt where tt.num == 0")
    val tempDF = ss.sql("select jan_code,fetch_time, row_number() OVER (PARTITION BY to_timestamp(fetch_time) ORDER BY to_timestamp(fetch_time)) as rank FROM global_temp.test ")

    val finalDF = tempDF
      .writeStream

      //      .option(ConstantUtils.CHECKPOINT_LOCATION, "/tmp/checkpoint")
      .outputMode("Update")
//      .foreach(new ForeachWriter[Row]() {
//        override def open(partitionId: Long, epochId: Long): Boolean = {
//          true
//        }
//
//        override def process(value: Row): Unit = {
//          tempDF.explain()
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//
//        }
//      })
      .format("console")
      .trigger(Trigger.ProcessingTime(10000))
      .start()
    finalDF.explain()
    finalDF.awaitTermination()
  }
}
