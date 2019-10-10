import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object SparkReadText {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("ReadFromText").getOrCreate()
//    val df = spark.read.parquet("hdfs://10.16.26.204:8020/user/hive/warehouse/spark_stream_platform.db/parquet_table")
    val df = spark.read.load("")
    df.show()
//    val tmpDF = spark.read.textFile("file:///D:\\MyData\\xiehy11\\Desktop\\自己资料\\stedent_INFO.txt")
//    val colNameArr = "id,name,sex,height,test".split(",")
//    var transformDF = tmpDF.selectExpr("CAST(value AS STRING) as value")
//      .withColumn("splitCols", functions.split(col("value"), ","))
//    //给每子列赋名称
//    colNameArr.zipWithIndex.foreach(
//      x => {
//      transformDF = transformDF.withColumn(x._1, col("splitCols").getItem(x._2))
//      println("=================================")
//    })
//    val resultDF = transformDF.drop("value").drop("splitCols")
//    resultDF.select()
//    resultDF.write.csv("file:///D:\\MyData\\xiehy11\\Desktop\\自己资料\\stedent_INFO2")
//    println("xxxxxxxxxxxxxxxxxxxxx")
  }

}
