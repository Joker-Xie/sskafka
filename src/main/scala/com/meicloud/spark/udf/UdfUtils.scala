/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package com.meicloud.spark.udf

import java.lang.reflect.Method

import com.meicloud.spark.entity.CaseVo._
import com.meicloud.spark.log.EdpLogging
import com.meicloud.spark.utils.ConstantUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable

object UdfUtils extends EdpLogging {


  lazy val jarPathMap = mutable.HashMap.empty[String, (Any, Method)]

  def getObjectAndMethod(udfName: String, className: String): (Any, Method) = {
    if (!jarPathMap.contains(udfName)) {
      val clazz = Class.forName(className)
      val o: Any = clazz.newInstance()
      val methods = clazz.getMethods
      var callMethod: Method = null
      for (i <- methods.indices) {
        val m: Method = methods(i)
        if (m.getName.equals(udfName)) {
          callMethod = m
        }
      }
      logInfo("reflect getMethod")
      jarPathMap(udfName) = (o, callMethod)
    }
    jarPathMap(udfName)
  }

  def convertSparkType(returnClassName: String) = {
    logInfo("convertSparkType:"+returnClassName)
    returnClassName match {
      case "int" => IntegerType
      case "java.lang.Integer" => IntegerType
      case "long" => LongType
      case "java.lang.Long" => LongType
      case "float" => FloatType
      case "java.lang.Float" => FloatType
      case "double" => DoubleType
      case "java.lang.Double" => DoubleType
      case "boolean" => BooleanType
      case "java.lang.Boolean" => BooleanType
      case "java.lang.String" => StringType
      case "java.math.BigDecimal" => DecimalType.SYSTEM_DEFAULT
      case "java.util.Date" => DateType
      case "java.sql.Date" => DateType
      case "java.sql.Timestamp" => TimestampType
      case "java.security.Timestamp" => TimestampType
      case "com.sun.jmx.snmp.Timestamp" => TimestampType
      case _ => BinaryType
    }
  }

  def removeUdf(udfName: String): Unit = {
    jarPathMap.remove(udfName)
  }

  def registerUDF(udfClassMethodName:String,
                  udfFullClassName:String,sparkSql:String,spark:SparkSession): Unit ={
    val udfConfigArr = new scala.Array[UdfConfig](1)
    val udfConfig = new UdfConfig(udfClassMethodName,udfFullClassName)
    udfConfigArr.update(0,udfConfig)
    val batchJobConfig = new BatchJobConfig(udfConfigArr.toList,sparkSql);

    //注册UDF函数
    if (batchJobConfig.udfConfig.nonEmpty && batchJobConfig.udfConfig.nonEmpty) {
      batchJobConfig.udfConfig.foreach(udf => {
        UdfRegister.register(udf.udfName, udf.udfClassFullname, spark)
      })
    }
  }

  //注册UDF函数
  def registerUDF(configMap:(String,ExecutorConfigVo),spark:SparkSession): Unit ={
      configMap._1 match {
         case ConstantUtils.EXECUTE_SQL => {
           val exeSqlConfigVo = configMap._2.asInstanceOf[ExeSqlConfigVo]
//           println(">>>>>>>>>>>>exeSqlConfigVo:"+exeSqlConfigVo)
           val udfList = exeSqlConfigVo.udfList
           udfList.map{vo:UDFConfigVo =>
              registerUDF(vo.classMethod,vo.classPath,exeSqlConfigVo.sqlContent,spark)
           }
         }
         case ConstantUtils.EXECUTE_SCALA => None
         case ConstantUtils.EXECUTE_PYTHON => None
         case  _ => None
       }
  }
}
