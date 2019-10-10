package com.meicloud.spark.utils

/**
  * Created by yesk on 2019-3-25.
  * 描述:MySQL DDL和DML工具类
  * 日期:创建于2019-03-25 12:43
  */

import java.sql.{Connection, Date, SQLException, Timestamp}
import java.util.Properties

import com.meicloud.saprk.hdfs.utils.JavaStringUtils
import com.meicloud.spark.stream.kafka.KafkaToKafka
import com.meicloud.spark.utils.MySQLPoolManager
//import com.github.binarywang.java.emoji.EmojiConverter
import com.meicloud.spark.entity.CaseVo.{JobConfigVo, OutputJdbcConfigVo, ProcessStateVo}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

object MySQLUtils {
  val logger: Logger = Logger.getLogger(getClass.getSimpleName)

  /**
    * 将DataFrame所有类型(除id外)转换为String后,通过c3p0的连接池方法,向mysql写入数据
    *
    * @param  outputJdbcConfigVo
    * @param resultDateFrame
    */
  def saveDFtoDBUsePool(outputJdbcConfigVo: OutputJdbcConfigVo, resultDateFrame: DataFrame): Unit = {
    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertSql(outputJdbcConfigVo, colNumbers)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "％", outputJdbcConfigVo.jdbcTable, "％") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始,record.getString()方法从0开始
          for (i <- 1 to colNumbers) {
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if (value != null) {
              //如何值不为空,将类型转换为String
              preparedStatement.setString(i, value.toString)
              dateType match {
                case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: LongType => preparedStatement.setLong(i, record.getAs[Long](i - 1))
                case _: BooleanType => preparedStatement.setBoolean(i, record.getAs[Boolean](i - 1))
                case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](i - 1))
                case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](i - 1))
                case _: StringType => preparedStatement.setString(i, record.getAs[String](i - 1))
                case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                case _: DateType => preparedStatement.setDate(i, record.getAs[Date](i - 1))
                case _ => throw new RuntimeException(s"nonsupport ${dateType} !!!")
              }
            } else {
              //如果值为空,将值设为对应类型的空值
              metaData.absolute(i)
              preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(s"@@ saveDFtoDBUsePool ${e.getMessage}")
        //做一些log 
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }

  /**
    * 拼装插入SQL
    *
    * @param outputJdbcConfigVo
    * @param colNumbers
    * @return
    */
  def getInsertSql(outputJdbcConfigVo: OutputJdbcConfigVo, colNumbers: Int): String = {
    var sqlStr = "insert into" + outputJdbcConfigVo.jdbcTable + "values("
    for (i <- 1 to colNumbers) {
      sqlStr += "?"
      if (i != colNumbers) {
        sqlStr += ","
      }
    }
    sqlStr += ")"
    sqlStr
  }


  /**
    * 从MySQL的数据库中获取DateFrame
    *
    * @param sqlContext SQLContext
    * @param outputJdbcConfigVo
    * @param queryCondition
    * @return DateFrame
    */
  def getDFFromMysql(sqlContext: SQLContext, outputJdbcConfigVo: OutputJdbcConfigVo, queryCondition: String): DataFrame = {
    val (jdbcURL, userName, passWord) = (outputJdbcConfigVo.jdbcUrl, outputJdbcConfigVo.jdbcUser, outputJdbcConfigVo.jdbcPass)
    val prop = new Properties()
    prop.put("user", userName)
    prop.put("password", passWord)

    if (null == queryCondition || "" == queryCondition)
      sqlContext.read.jdbc(jdbcURL, outputJdbcConfigVo.jdbcTable, prop)
    else
      sqlContext.read.jdbc(jdbcURL, outputJdbcConfigVo.jdbcTable, prop).where(queryCondition)
  }

  /**
    * 删除数据表
    *
    * @param sqlContext
    * @param outputJdbcConfigVo
    * @return
    */
  def dropMysqlTable(outputJdbcConfigVo: OutputJdbcConfigVo, sqlContext: SQLContext): Boolean = {
    val conn = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection //从连接池中获取一个连接
    val preparedStatement = conn.createStatement()
    try {
      preparedStatement.execute(s"drop table ${outputJdbcConfigVo.jdbcTable}")
    } catch {
      case e: Exception =>
        println(s"mysql dropMysqlTable error:${e.getMessage}")
        false
    } finally {
      preparedStatement.close()
      conn.close()
    }
  }

  /**
    * 删除表中的数据
    *
    * @param sqlContext
    * @param outputJdbcConfigVo
    * @param condition
    * @return
    */
  def deleteMysqlTableData(outputJdbcConfigVo: OutputJdbcConfigVo, sqlContext: SQLContext, condition: String): Boolean = {
    val conn = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection //从连接池中获取一个连接
    val preparedStatement = conn.createStatement()
    try {
      preparedStatement.execute(s"从${outputJdbcConfigVo.jdbcTable}中删除${condition}")
    } catch {
      case e: Exception =>
        println(s" mysql deleteMysqlTable错误:${e.getMessage}")
        false
    } finally {
      preparedStatement.close()
      conn.close()
    }
  }

  /**
    * 保存DataFrame到MySQL中,如果表不存在的话,会自动创建
    *
    * @param outputJdbcConfigVo
    * @param resultDateFrame
    */
  def saveDFtoDBCreateTableIfNotExist(outputJdbcConfigVo: OutputJdbcConfigVo, resultDateFrame: DataFrame) {
    //如果没有表,根据DataFrame建表
    createTableIfNotExist(outputJdbcConfigVo, resultDateFrame)
    //验证数据表字段和dataFrame字段个数和名称,顺序是否一致
    verifyFieldConsistency(outputJdbcConfigVo, resultDateFrame)
    //保存df 
    saveDFtoDBUsePool(outputJdbcConfigVo, resultDateFrame)
  }


  /**
    * 拼装insertOrUpdate SQL语句
    *
    * @param tableName
    * @param cols
    * @param updateColumns
    * @return
    */
  def getInsertOrUpdateSql(tableName: String, cols: Array[String], updateColumns: Array[String]): String = {
    val colNumbers = cols.length
    var sqlStr = "insert into " + tableName + " values("
    for (i <- 1 to colNumbers) {
      sqlStr += "?"
      if (i != colNumbers) {
        sqlStr += ","
      }
    }
    sqlStr += ") ON DUPLICATE KEY UPDATE "

    updateColumns.foreach(str => {
      sqlStr += s" ${str}=?,"
    })
    sqlStr.substring(0, sqlStr.length - 1)
  }

  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中,注意:此方式,必须对表设置主键
    *
    * @param outputJdbcConfigVo
    * @param resultDateFrame
    * @param updateColumns
    */
  def insertOrUpdateDFtoDBUsePool(outputJdbcConfigVo: OutputJdbcConfigVo, resultDateFrame: DataFrame, updateColumns: Array[String], jobConfigVo: JobConfigVo, processConf: ProcessStateVo) {

    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertOrUpdateSql(outputJdbcConfigVo.jdbcTable, resultDateFrame.columns, updateColumns)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
    println("## ############ sql =" + sql)
    resultDateFrame.foreachPartition(
      partitionRecords => {
        var batchSize = 0
        val conn = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection //从连接池中获取一个连接
        val preparedStatement = conn.prepareStatement(sql)
        val metaData = conn.getMetaData.getColumns(null, "％", outputJdbcConfigVo.jdbcTable, "％") //通过连接获取表名对应数据表的元数据
        var cont = 0
        try {
          conn.setAutoCommit(false)
          partitionRecords.foreach(
            record => {
              cont += 1
              batchSize += 1
              //注意:setString方法从1开始,record.getString()方法从0开始
//              println(">>>>>>>>>>>>>>> colNumbers: "+colNumbers)
              for (i <- 1 to colNumbers if record != null) {
                val value = record.get(i - 1)
                val dateType = columnDataTypes(i - 1)
                if (value != null) {
                  //如何值不为空,将类型转换为String
                  preparedStatement.setString(i, value.toString)
                  dateType match {
                    case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                    case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                    case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                    case _: LongType => preparedStatement.setLong(i, record.getAs[Long](i - 1))
                    case _: BooleanType => preparedStatement.setInt(i, if (record.getAs[Boolean](i - 1)) 1 else 0)
                    case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](i - 1))
                    case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](i - 1))
                    //将表情数据转成utf8兼容的数据格式
                    case _: StringType => preparedStatement.setString(i, JavaStringUtils.parserEmojiStr(record.getAs[String](i - 1)))
                    case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                    case _: DateType => preparedStatement.setDate(i, record.getAs[Date](i - 1))
                    case _: StructType => preparedStatement.setObject(i, record.getAs[Object](i - 1))
                    case _ => throw new RuntimeException(s"nonsupport ${dateType} !!!")
                  }
                } else { //如果值为空,将值设为对应类型的空值
                  metaData.absolute(i)
                  preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
                }
              }
              //设置需要更新的字段值
              for (i <- 1 to updateColumns.length) {
                val fieldIndex = record.fieldIndex(updateColumns(i - 1))
                val value = record.get(fieldIndex)
                val dataType = columnDataTypes(fieldIndex)
                //            println(s"@@ $ fieldIndex,$ value,$ dataType")
                if (value != null) {
                  //如何值不为空,将类型转换为String
                  dataType match {
                    case _: ByteType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                    case _: ShortType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                    case _: IntegerType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                    case _: LongType => preparedStatement.setLong(colNumbers + i, record.getAs[Long](fieldIndex))
                    case _: BooleanType => preparedStatement.setBoolean(colNumbers + i, record.getAs[Boolean](fieldIndex))
                    case _: FloatType => preparedStatement.setFloat(colNumbers + i, record.getAs[Float](fieldIndex))
                    case _: DoubleType => preparedStatement.setDouble(colNumbers + i, record.getAs[Double](fieldIndex))
                    case _: StringType => preparedStatement.setString(colNumbers + i, record.getAs[String](fieldIndex))
                    case _: TimestampType => preparedStatement.setTimestamp(colNumbers + i, record.getAs[Timestamp](fieldIndex))
                    case _: DateType => preparedStatement.setDate(colNumbers + i, record.getAs[Date](fieldIndex))
                    case _ => throw new RuntimeException(s"nonsupport ${dataType} !!!")
                  }
                } else {
                  //如果值为空,将值设为对应类型的空值
                  metaData.absolute(colNumbers + i)
                  preparedStatement.setNull(colNumbers + i, metaData.getInt("DATA_TYPE"))
                }
              }
              preparedStatement.addBatch()
              //设置batchSize为3000提交一次，避免批次过大
              if (batchSize == 1000) {
                preparedStatement.executeBatch()
                conn.commit()
                batchSize = 0
                Thread.sleep(2000)
              }
            })
          preparedStatement.executeBatch()
          conn.commit()
        } catch {
          case e: SQLException =>
            e.printStackTrace()
          //做一些log
        } finally {
          preparedStatement.close()
          conn.close()
        }
      })
    //    ps.writer.close()
  }


  /**
    * 如果数据表不存在,根据DataFrame的字段创建数据表,数据表字段顺序和dataFrame对应
    * 若DateFrame出现名为id的字段,将其设为数据库主键(int,自增,主键),其他字段会根据DataFrame的DataType类型来自动映射到MySQL中
    *
    * @param outputJdbcConfigVo
    * @param df dataFrame
    * @return
    */
  def createTableIfNotExist(outputJdbcConfigVo: OutputJdbcConfigVo, df: DataFrame): AnyVal = {
    val con = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection
    val metaData = con.getMetaData
    val colResultSet = metaData.getColumns(null, "％", outputJdbcConfigVo.jdbcTable, "％")
    //如果没有该表,创建数据表
    if (!colResultSet.next()) {
      //构建建表字符串
      val sb = new StringBuilder(s"CREATE TABLE ")
        .append(outputJdbcConfigVo.jdbcTable).append(" (")

      df.schema.fields.foreach(x =>
        if (x.name.equalsIgnoreCase("id")) {
          sb.append(x.name).append(s" int(255) NOT NULL AUTO_INCREMENT PRIMARY KEY,") //如果是字段名为id,设置主键,整形,自增
        } else {
          x.dataType match {
            case _: ByteType => sb.append(x.name).append(s" int(100)DEFAULT NULL,")
            case _: ShortType => sb.append(x.name).append(s" int(100)DEFAULT NULL,")
            case _: IntegerType => sb.append(x.name).append(s" int(100)DEFAULT NULL,")
            case _: LongType => sb.append(x.name).append(s" bigint(100)DEFAULT NULL,")
            case _: BooleanType => sb.append(x.name).append(s"  tinyint DEFAULT NULL,")
            case _: FloatType => sb.append(x.name).append(s" float(50)DEFAULT NULL,")
            case _: DoubleType => sb.append(x.name).append(s" double(50)DEFAULT NULL,")
            case _: StringType => sb.append(x.name).append(s" varchar (50)DEFAULT NULL,")
            case _: TimestampType => sb.append(x.name).append(s" timestamp DEFAULT current_timestamp,")
            case _: DateType => sb.append(x.name).append(" date DEFAULT NULL,")
            case _ => throw new RuntimeException(s"nonsupport ${x.dataType}!!!")
          }
        }
      )
      sb.append(")ENGINE = InnoDB DEFAULT CHARSET = utf8")
      val sql_createTable = sb.deleteCharAt(sb.lastIndexOf(',')).toString()
      println(sql_createTable)
      val statement = con.createStatement()
      statement.execute(sql_createTable)
    }
  }

  /**
    * 验证数据表和dataFrame字段个数,名称,顺序是否一致
    *
    * @param outputJdbcConfigVo
    * @param df dataFrame
    */
  def verifyFieldConsistency(outputJdbcConfigVo: OutputJdbcConfigVo, df: DataFrame): Unit = {
    val con = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection
    val metaData = con.getMetaData
    val colResultSet = metaData.getColumns(null, "％", outputJdbcConfigVo.jdbcTable, "％")
    colResultSet.last()
    val tableFiledNum = colResultSet.getRow
    val dfFiledNum = df.columns.length
    if (tableFiledNum != dfFiledNum) {
      throw new Exception(s"数据表和DataFrame字段个数不一致!! table-${tableFiledNum}但dataFrame-${dfFiledNum}")
    }
    for (i <- 1 to tableFiledNum) {
      colResultSet.absolute(i)
      val tableFileName = colResultSet.getString("COLUMN_NAME")
      val dfFiledName = df.columns.apply(i - 1)
      if (!tableFileName.equals(dfFiledName)) {
        throw new Exception("一个或多个数据表和DataFrame字段名不一致!! table - ${tableFileName}但dataFrame - ${dfFiledName}")
      }
    }
    colResultSet.beforeFirst()
  }

} 
