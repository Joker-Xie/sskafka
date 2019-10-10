package com.meicloud.spark.utils

import java.util

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor
import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.Row

/**
  * Created by yesk on 2019-3-6. 
  */
object StringUtils {

  //判断数据是否都为null
  def isAllNull(record: Row) = {
    val len = record.length
    var allNull = true
    for (i <- 0 until len) {
      allNull = allNull && record.isNullAt(i)
    }
    allNull
  }

  //获取isAllNull 的
  def getIsAllNull(str: String): String = {
    val sp = str.indexOf("java.lang.Exception") + 21
    str.substring(sp, sp + 5).trim
  }

  def getSqlColAlias(sql: String, udfName: String): util.ArrayList[String] = {
    val aliasList = new util.ArrayList[String]()
    val firstSplit = sql.split(udfName.trim)
    if (firstSplit.size >= 2) {
      for (i <- 1 until firstSplit.size) {
        val aliasName = firstSplit(i).split("\\)\\s+[a|A][s|S]")(1).split("[\\s+|,]")(1)
        aliasList.add(aliasName)
      }
    }
    aliasList
    //    sql.split(udfName.trim + "\\(.*\\)\\s+[a|A][s|S]")(1).trim.split("[\\s+|,]")(0)
  }


  def appendGlobalTempDbName(sql: String): String = {
    val dbType = JdbcConstants.HIVE
    val sqlList = SQLUtils.parseStatements(sql, dbType)
    var opsSql = sql
    for (i <- 0 until sqlList.size()) {
      val stmt = sqlList.get(i)
      val version = new MySqlSchemaStatVisitor()
      stmt.accept(version)
      val itor = version.getTables().keySet().iterator()
      while (itor.hasNext) {
        val singleTabName = itor.next().toString
        opsSql = opsSql.replace(singleTabName, "global_temp." + singleTabName)
      }
    }
    opsSql
    /* val parser = new MySqlStatementParser(sql)
     val statement = parser.parseStatement
     val vision = new MySqlSchemaStatVisitor()
     statement.accept(vision)
     var opsSql = sql
     val itor = vision.getTables().keySet().iterator()
     while (itor.hasNext) {
       val singleTabName = itor.next().toString
       opsSql = opsSql.replace(singleTabName, "global_temp." + singleTabName)
     }
     opsSql*/
  }

  def appendTopicAndPartAndOffset(sql: String): String = {
    sql.replaceAll("select", "select topic,partition,offset,")
      .replaceAll("Select", "Select topic,partition,offset,")
      .replaceAll("SELECT", "SELECT topic,partition,offset,")
  }

  def isJSONOfKafkaDataType(dataType: String): Boolean = {
    dataType.trim.toLowerCase.equals("json")
  }

  def isTextOfKafkaDataType(dataType: String): Boolean = {
    dataType.trim.toLowerCase.equals("text")
  }

  def optimizeSql(sql: String): String = {
    if (sql.toUpperCase.contains(".FROM")) {
      sql.toUpperCase.replaceAll("\\.FROM", ".OTHER")
    }
    else {
      if (sql.toUpperCase.split("FROM").length > 2) {
        sql.toUpperCase.replaceFirst("FROM", "OTHER")
      }
      else {
        sql
      }
    }
  }

  def getKafkaTmpTableName(sql: String, tableList: java.util.ArrayList[String]): String = {
    val dbType = JdbcConstants.HIVE
    val sqlList = SQLUtils.parseStatements(sql, dbType)
    var opsSql = sql
    val allTabName = new util.ArrayList[String]()
    for (i <- 0 until sqlList.size()) {
      val stmt = sqlList.get(i)
      val version = new MySqlSchemaStatVisitor()
      stmt.accept(version)
      val itor = version.getTables().keySet().iterator()
      while (itor.hasNext) {
        allTabName.add(itor.next().toString)
      }
    }
    if (tableList != null) {
      allTabName.removeAll(tableList)
    }
    allTabName.get(0)
  }

  def getKafkaIPPort(ip: String, port: String): String = {
    ip.replaceAll(" ", "").replaceAll("，", ",").replaceAll(",", ":" + port + ",").concat(":" + port)

  }

  def hasEmptyElement(arr: Array[String]): Boolean = {
    arr.filter(x => null == x || "".equals(x)).size > 0
  }

  def getTriggerInteval(trigger: Int): String = {
    if (trigger > 1) trigger + " " + ConstantUtils.TRIGGER_UNIT_SECONDS else trigger + " " + ConstantUtils.TRIGGER_UNIT_SECOND
  }

  def getTimeNum(time: String): String = {
    time.replace("-", "").replace(":", "").replace(" ", "").substring(0, 14)
  }


  def matchValue(dataTypes: Array[DataType], record: GenericRowWithSchema, i: Int): Any = {


    dataTypes(i) match {
      case ByteType => record.getAs[String](i)
      case ShortType => record.getAs[Int](i)
      case IntegerType => record.getAs[Int](i)
      case LongType => record.getAs[Long](i)
      case BooleanType => record.getAs[Boolean](i)
      case FloatType => record.getAs[Float](i)
      case DoubleType => record.getAs[Double](i)
      case StringType => " \"" + StringEscapeUtils.escapeJava(record.getAs[String](i)) + "\""
      case TimestampType => record.getAs[String](i)
      case DateType => record.getAs[String](i)
      case _ => record.getAs[String](i)
    }
  }

  def involuteRecord(record: GenericRowWithSchema): String = {
    val nameCells = record.schema.fields.map(_.name)
    val dataTypes = record.schema.fields.map(_.dataType)
    var values = ""
    for (i <- 1 to nameCells.length - 1) {
      if (i < nameCells.length - 1) {
        values = values + "\"" + nameCells(i) + "\" :" + matchValue(dataTypes, record, i) + ","
      }
      else {
        values = values + "\"" + nameCells(i) + "\" :" + matchValue(dataTypes, record, i)
      }
    }
    println("{" + values + "}")
    "{" + values + "}"
  }

  def main(args: Array[String]): Unit = {
    val listName = new util.ArrayList[String]()
    listName.add("table1")
    listName.add("table2")
    val sql = "select t1.id ,t2.id,t3.id from (select id ,name,age from table1)as t1 left join table2 as t2 on t1.id = t2.id left join table3 as t3 on t1.id = t3.id"
    appendGlobalTempDbName(sql)

    //    val udfName = "toAnalyzer"
    //    val str = getSqlColAlias(sql, udfName)
    //    println("xxxxx")
    //    val str = "2019-03-27 09:29:52.0"
    //    getTimeNum(str)
    //    val sql = "select src ,date ,content ,url ,taskId ,picNum ,tags ,itemId ,subItemTid  ,mark  ,price  ,userName  ,userArea  ,userLevel ,buyFlag  ,usefulCnt  ,sendPlatform  ,props {},shopTid  ,model  ,brand  ,category  ,fetchtime  ,shopId  ,categoryId  ,shopName  ,rootCatId  ,itemCatId  ,piclinks  ,id  ,redo  ,from  ,to  ,toAnalyzer(category,content,'http://10.18.29.120:9080/analyzer/textAnalyzer.do') as netCol,commentType from comment_white_redo"
    //    println(getKafkaIPPort("10.16.26.201,10.16.26.202,10.16.26.203","9092"))
    //    println(getSqlColAlias(sql,"toAnalyzer"))
    //    " netcol from  tbl  ".trim.split("\\s+").foreach(println)

  }
}
