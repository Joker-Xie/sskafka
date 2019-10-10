import java.sql.DriverManager

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkOptionHive {

  case class student (id:Int,name:String,age:Int)
  def main(args: Array[String]): Unit = {

    val ssc = SparkSession.builder().master("local").appName("readParQuen").getOrCreate()

    val df = ssc.read.parquet("D:\\usr\\part-00004-5f09de40-a89b-4c35-a3b0-cbabbab2f11c-c000.snappy.parquet")
    df.show()
//    val nameRDD = ssc.sparkContext.makeRDD(Array(
//      "{\"id\":2,\"name\":\"zhangsan\",\"age\":18}",
//      "{\"id\":3,\"name\":\"lisi\",\"age\":19}",
//      "{\"id\":4,\"name\":\"wangwu\",\"age\":20}"
//    ))
//    import ssc.implicits._
//    val nameDF = ssc.createDataset(nameRDD)
//    nameDF.write.parquet("people.parquet")
//    peopleDF.write().parquet("people.parquet");
//    nameDF.write.parquet("D:\\MyData\\xiehy11\\Desktop\\student\\000000_0_copy_1\test.parquet")
    //    Class.forName("org.apache.hive.jdbc.HiveDriver")
    //    val conn = DriverManager.getConnection("jdbc:hive2://10.16.26.203:10001")
    //    val sql = "insert into spark_stream_platform.student values(1,'tom2',16)"
    //    val st = conn.createStatement()
    //    st.execute("use spark_stream_platform")
    //    st.execute(sql)
    //    st.execute("use spark_stream_platform")
    //    val rs = st.executeQuery("select * from spark_stream_platform.student")
    //    while (rs.next()){
    //      println(rs.getString(1)+"  "+rs.getString(2)+"  "+rs.getString(3))
    //    }
    //    st.close()
    //    conn.close()
  }
}
