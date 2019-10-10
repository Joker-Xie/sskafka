import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor
import com.meicloud.spark.offset.redis.RedisOffsetOperate
import com.meicloud.spark.offset.redis.RedisOffsetOperate.OffsetMsg
import com.meicloud.spark.utils.JedisConnectionPool
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.kafka.common.config.Config
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.apache.spark.sql.types._

import scala.util.control.Breaks
import org.json4s._
import org.json4s.jackson.Json

import scala.collection.mutable

object Json4sTest {

  abstract class A(name: String, age: Int)

  case class B() extends A("a", 12)

  def main(args: Array[String]): Unit = {
    val a :A = B().asInstanceOf[A]
    val b: B = a.asInstanceOf[B]
    println(b.asInstanceOf[B])
    println(b.asInstanceOf[A])

    //    val sql = "select * from (select explode(order1.itemsinfo.cnt) as test from order_info )  as test1"
    //    val parser = new MySqlStatementParser(sql)
    //    val statement = parser.parseStatement
    //    val vision =  new MySqlSchemaStatVisitor()
    //    statement.accept(vision)
    //    val obj = vision.getTables().keySet().toArray()(0)
    //    println("xxxx")
    //    val tableName = StringUtil.removeBackquote(insert.getTableName.getSimpleName)


    //    import com.github.binarywang.java.emoji.EmojiConverter
    //    val emojiConverter = EmojiConverter.getInstance
    //    val str = "还没用，不知道，但是送货小哥非常棒，五楼一口气爬上来，辛苦啦！😝😝"
    //    val rs = emojiConverter.toAlias(str)
    //    val rs1 = emojiConverter.toUnicode(str)


    //    val str = "{\"+a+\"+"+":\"+b+\"}"
    //    val jv = parse(str)
    //    println("xxxxxxx")
    //    val loop = "false"
    //    val rs = loop.toBoolean
    //
    ////    var count = 0
    ////      for (i <- 0 to 10) {
    ////        println(i)
    ////        if (count == 5) {
    ////          return
    ////        }
    ////        count += 1
    ////      }
    //    val conn = JedisConnectionPool.getConnections()

    //    while (true) {
    //      val rs  = RedisOffsetOperate.getLastCommittedOffsets("gxt.comment.white.redo.test","gxt.test.white.redo.flow_test13")

    //      val object_1 = RedisSeriaUtils.unserializableObject(conn.get("gxt.test.white.redo.flow_test1".getBytes())).asInstanceOf[OffsetMsg]
    //      val rs = object_1.offsetObj.get("gxt.test.white.redo.flow_test1")
    //      val m = object_1.offsetObj

    //      val conn = JedisConnectionPool.getConnections()
    //      //      val keys = "job_kafka2es_test_sample&gxt.comment.white.redo.test".split("&")
    //      val keys = "job_kafka2es_test_sample:gxt.comment.white.redo.test"
    //      val str = conn.hgetAll("job_kafka2es_test_sample:gxt.comment.white.redo.test")
    //      val itr = str.entrySet().iterator()
    //      var rs = ""
    //      while (itr.hasNext) {
    //        val key = itr.next()
    //        rs = rs + "\"" + key.getKey + "\":" + key.getValue + ","
    //      }
    //      //      println("{\""+keys(1)+"\":{" + rs.substring(0,rs.length-1) + "}}")
    //      println("{" + keys + rs.substring(0, rs.length - 1) + "}}")
    //      Thread.sleep(3000)
    //    }
  }
}
