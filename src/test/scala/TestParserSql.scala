import java.util.Date

import com.meicloud.spark.utils.StringUtils
import org.apache.commons.lang3.time.FastDateFormat

object TestParserSql {
  def createOrderInfo(str: String): String = {
    val mark = str.trim.toLowerCase
    var rs: String = null
    if (mark.endsWith("regular")) {
      rs = createRegularOrderInfo()
    }
    else if (mark.endsWith("random")) {
      rs = createRandomOrderInfo()
    }
    else {
      rs = null
    }
    rs
  }


  def main(args: Array[String]): Unit = {
    val order = createOrderInfo("random")
    println(order)
  }

  def createRegularOrderInfo(): String = {
    val uuid = java.util.UUID.randomUUID().toString.replaceAll("-", "")
    val order = StringBuilder.newBuilder.append("{\"order1\": {\"uid\": \"")
      .append(uuid)
      .append("\",\"time\":\"1234\",\"itemsinfo\":[{\"shopid\": \"shopid9999\",\"itemid\": \"itemid9999\",\"cnt\":10,\"price\": 120,\"cut\": 10},{\"shopid\": \"shopid8888\",\"itemid\": \"itemid8888\",\"cnt\":10,\"price\": 130,\"cut\": 5},{\"shopid\": \"shopid7777\",\"itemid\": \"itemid7777\",\"cnt\":10,\"price\": 110,\"cut\": 0}]}}")
    order.toString()
  }

  def createRandomOrderInfo(): String = {
    val price1 = scala.util.Random.nextInt(30)
    val price2 = scala.util.Random.nextInt(30)
    val price3 = scala.util.Random.nextInt(30)
    val shopid1 = scala.util.Random.nextInt(100)
    val shopid2 = scala.util.Random.nextInt(100)
    val shopid3 = scala.util.Random.nextInt(100)
    val itemid1 = scala.util.Random.nextInt(100)
    val itemid2 = scala.util.Random.nextInt(100)
    val itemid3 = scala.util.Random.nextInt(100)
    val uuid = java.util.UUID.randomUUID().toString.replaceAll("-", "")
    //      var now: Date = new Date()
    //      var dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    //      val today = dateFormat.format(now)
    val today = System.currentTimeMillis()
    //      var order = "{\"order\": {\"uid\": \"uid1\",\"time\": %s\",\"itemsinfo\": [{\"shopid\": \"shopid1\",\"itemid\": \"itemid1\",\"cnt\": %d,\"price\": 120,\"cut\": 10},{\"shopid\": \"shopid1\",\"itemid\": \"itemid2\",\"cnt\": %d,\"price\": 130,\"cut\": 5},{\"shopid\": \"shopid2\",\"itemid\": \"itemid3\",\"cnt\": %d,\"price\": 110,\"cut\": 0}]}}"
    var order = StringBuilder.newBuilder
    order.append("{\"order1\": {\"uid\": \"").append(uuid).append("\",\"time\":\"").append(today)
      .append("\",\"itemsinfo\": [{\"shopid\": \"shopid").append(shopid1)
      .append("\",\"itemid\": \"itemid").append(itemid1).append("\",\"cnt\":").append(price1)
      .append(",\"price\": 120,\"cut\": 10},{\"shopid\": \"shopid").append(shopid2)
      .append("\",\"itemid\": \"itemid").append(itemid2).append("\",\"cnt\":").append(price2)
      .append(",\"price\": 130,\"cut\": 5},{\"shopid\": \"shopid").append(shopid3)
      .append("\",\"itemid\": \"itemid").append(itemid3).append("\",\"cnt\":").append(price3).append(",\"price\": 110,\"cut\": 0}]}}")
    order.toString()
  }
}
