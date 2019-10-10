import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SparkWrite2hdfs {

  def main(args: Array[String]): Unit = {
    /*
    //删除hdfs数据
    val conf = new Configuration()
    val path = new Path("hdfs://10.16.26.204:8020/user/hive/warehouse/spark_stream_platform.db/parquent_table_test/20190916")
    val fs = path.getFileSystem(conf)
    fs.delete(path,true)
    println("finish!")
    */

    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkWrite2hdfs")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //    val inputDF = spark.read.json("D:\\MyData\\xiehy11\\Desktop\\test.json")
    //    inputDF.write.parquet("hdfs://10.16.26.203:9000/user/hive/warehouse/spark_stream_platform.db/parquent_table_test")
    //        inputDF.createOrReplaceTempView("test")
    //    spark.sql("select * from test").write
    //      .parquet("hdfs://10.16.26.204:8020/user/hive/warehouse/spark_stream_platform.db/parquent_table_test/part_dt=20190913")
    //      .parquet("hdfs://192.168.112.128:9000/spark/student")


    /**
      * JDBC
      */
        val url = "jdbc:mysql://localhost:3306/mydb"
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "123456")
        val staticDS1 = spark.read.jdbc(url, "test_table1", props)
    //    staticDS1.write.parquet("hdfs://192.168.112.128:50010/spark/data")
    //    staticDS1.write.parquet("hdfs://192.168.112.128:9000/spark/data")
    //    staticDS1.write.parquet("D:\\MyData\\xiehy11\\Desktop\\data")
    //    val partQuent =  spark.read.parquet("hdfs://192.168.112.128:9000/spark/data").createOrReplaceTempView("test")
        spark.sql("select @test := null, id,name,age,From from test where From is not null").show()
  }

}
