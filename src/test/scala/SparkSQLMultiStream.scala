import java.util.Properties

import com.meicloud.spark.utils.ConstantUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkSQLMultiStream {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MultiStream Application")
      .config("spark.sql.shuffle.partitions", 10)
      .master("local")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")


    //读取es
    val esDF = spark.read.format(ConstantUtils.ORG_ELASTICSEARCH_SPARK_SQL)
      .option(ConstantUtils.ES_NET_HTTP_AUTH_USER, "elastic")
      .option(ConstantUtils.ES_NET_HTTP_AUTH_PASS, "123456")
      .option(ConstantUtils.ES_NODES, "192.168.112.128,192.168.112.129,192.168.112.130")
      .option(ConstantUtils.ES_PORT, 9200)
      .option(ConstantUtils.ES_RESOURCE, "/json")
      .load()
    esDF.show()

    val inputDS = spark.readStream.format("kafka")
      .option(ConstantUtils.KAFKA_BOOTSTRAP_SERVER, "192.168.112.128:9092")
      .option("subscribe", "test")
      .option(ConstantUtils.STARTING_OFFSETS, "earliest")
      .option("group.id", "spark")
      .option(ConstantUtils.MAX_OFFSETS_PER_TRIGGER, "100")
      .load()
    val schema =
      StructType(
        StructField("id", StringType, true) ::
          StructField("name", StringType, false) ::
          StructField("age", StringType, false) :: Nil)
    val tmpDS = inputDS.selectExpr("CAST(value as STRING)")
      .select(from_json(col("value"), schema, ConstantUtils.JSON_OPTIONS).as("kafkaDataTempTable"))
      .select(col("kafkaDataTempTable.*"))
    tmpDS.createOrReplaceGlobalTempView("tableFirst")

//    val list = List[String]("staticTable")
//    val prop = new Properties()
//    prop.put("dataSourceType", "file")
//    prop.put("dataFileType", "csv")
//    prop.put("path", "D:\\MyData\\xiehy11\\Desktop\\name.csv")
//    val conf = AttachStreamConfVo(list, prop)
//    factory.matchAndRegistDataSource(spark, conf)
    //    val staticDS = spark.read.textFile("D:\\MyData\\xiehy11\\Desktop\\student.txt").select(from_json(col("value"), schema, ConstantUtils.JSON_OPTIONS).as("test")).select("test.*")
    //    staticDS.createOrReplaceGlobalTempView("staticTable")
    val url = "jdbc:mysql://localhost:3306/mydb"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")
    val staticDS1 = spark.read.jdbc(url, "t_user_info", props)
    staticDS1.write.parquet("hdfs://192.168.112.128:50010/spark")
    //      .read.csv("D:\\MyData\\xiehy11\\Desktop\\name.csv").toDF("id", "name", "age")
    staticDS1.createOrReplaceGlobalTempView("staticTable1")

    import spark.implicits._
    //    val rsDF = spark.sql("select t1.id,count(t1.id) from global_temp.tableFirst as t1 left join global_temp.staticTable as t2 on t1.id = t2.id left join global_temp.staticTable1 as t3 on t1.id = t3.id group by t1.id")
    //    val rsDF = spark.sql("select * from global_temp.tableFirst")
    val rsDF = spark.sql("select t1.id,t2.id,t3.id,* from global_temp.tableFirst as t1 left join global_temp.staticTable as t2 on t1.id =t2.id left join global_temp.staticTable1 as t3 on t1.id = t3.id")
    //    rsDF.show()
    //    global_temp.tableFirst,
    rsDF.writeStream.format("console")
      .foreachBatch((ds: Dataset[Row], _: Long) => {
        ds.foreachPartition(
          partitions => {
            partitions.foreach(
              record => {
                println(record.toString())
              }
            )
          }
        )
      }
      )
      //      .outputMode("Complete")
      .outputMode("Append")
      .start()
      .awaitTermination()
  }
}
