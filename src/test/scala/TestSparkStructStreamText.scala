import com.meicloud.spark.utils.ConstantUtils
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

object TestSparkStructStreamText {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[3]").appName("Application").getOrCreate()
    val inputDF = spark.readStream
      .format("kafka")
      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, "10.16.25.176:9092,10.16.26.201:9092,10.16.26.202:9092")
//      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, "10.18.126.43:9092,10.18.126.44:9092,10.18.126.45:9092,10.18.126.46:9092,10.18.126.47:9092")
      //测试topic
      .option("subscribe", "gxt.test.white.redo.flow_test1")
//            .option(ConstantUtils.CHECKPOINT_LOCATION, "/tmp/checkpoint/")
      .option(ConstantUtils.STARTING_OFFSETS, "earliest")
      .option(ConstantUtils.MAX_OFFSETS_PER_TRIGGER, "100")
      .load()
    import spark.implicits._
    val joinDF =spark.createDataset[String]("id,name,sex,height".split(",").toList)
    val schema = "id,name,sex,height".split(",")
    var tmpDF = inputDF.
      selectExpr("CAST(value AS STRING) as value")
      .withColumn("splitCols", functions.split(col("value"), "&"))
        .withColumn("test",when($"value".isNull, lit(null))
          .otherwise("hello")
          .cast(StringType))
      .withColumn("test2",when($"value".isNull, lit(null))
      .otherwise("hello1")
      .cast(StringType))

    //给每子列赋名称
    schema.zipWithIndex.foreach(
      x => {
      tmpDF = tmpDF.withColumn(x._1, col("splitCols").getItem(x._2))
    })
    tmpDF = tmpDF.drop("value").drop("splitCols")
      .selectExpr("concat_ws('&',*) as value").select("value")
    tmpDF.writeStream.outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
