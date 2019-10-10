/*-
 * UDF 函数注册
 * add by lujh13
 */

package com.meicloud.spark.udf

import java.lang.reflect.Method

import com.meicloud.spark.log.EdpLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types._

object UdfRegister extends EdpLogging {

  /**
    * 通过反射 加载UDF 函数包
    * @param udfName udf函数名称
    * @param udfClassFullname udf函数全类名
    * @param session 会话
    */
  def register(udfName: String, udfClassFullname: String, session: SparkSession) {
    synchronized {
      val clazz = Class.forName(udfClassFullname)
      val method = {
        val methods = clazz.getMethods
        var callMethod: Method = null
        for (i <- methods.indices) {
          val m: Method = methods(i)
          if (m.getName.equals(udfName)) {
            callMethod = m
          }
        }
        callMethod
      }
      val returnDataType = UdfUtils.convertSparkType(method.getReturnType.getName)
      val inputPutDataType = method.getParameterTypes.map((x)=>UdfUtils.convertSparkType(x.getName))
      val inputPutDataNull =  method.getParameterTypes.map((x)=>false)
      val paramCount = method.getParameterCount
      registerUdf(paramCount, session, udfName, udfClassFullname, returnDataType, inputPutDataType, inputPutDataNull)
    }
  }

  /**
    * udf函数注册映射
    * @param paramCount  入参个数
    * @param session  会话
    * @param udfName  udf函数名称
    * @param udfClassName  udf全类名称
    * @param returnDataType  返回参数类型
    * @param inputPutDataType 输入参数类型
    * @param inputPutDataNull 输入数参数非空限制
    */
  private def registerUdf(paramCount: Int, session: SparkSession, udfName: String, udfClassName: String, returnDataType: DataType, inputPutDataType:Seq[DataType], inputPutDataNull: Seq[Boolean]) : Unit = {
    val udf = FunctionIdentifier(udfName)
    paramCount match {
      case 0 =>
        val f = new Function0[Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(): Any = method.invoke(o)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 1 =>
        val f = new Function1[Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s: Object): Any = method.invoke(o, s)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 2 =>
        val f = new Function2[Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object): Any = method.invoke(o, s1, s2)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 3 =>
        val f = new Function3[Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object): Any = method.invoke(o, s1, s2, s3)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 4 =>
        val f = new Function4[Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object): Any = method.invoke(o, s1, s2, s3, s4)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 5 =>
        val f = new Function5[Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object): Any = method.invoke(o, s1, s2, s3, s4, s5)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 6 =>
        val f = new Function6[Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 7 =>
        val f = new Function7[Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 8 =>
        val f = new Function8[Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 9 =>
        val f = new Function9[Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 10 =>
        val f = new Function10[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 11 =>
        val f = new Function11[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 12 =>
        val f = new Function12[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 13 =>
        val f = new Function13[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 14 =>
        val f = new Function14[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 15 =>
        val f = new Function15[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 16 =>
        val f = new Function16[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 17 =>
        val f = new Function17[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 18 =>
        val f = new Function18[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 19 =>
        val f = new Function19[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 20 =>
        val f = new Function20[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 21 =>
        val f = new Function21[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
      case 22 =>
        val f = new Function22[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object, s22: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22)
        }
        session.sessionState.functionRegistry.registerFunction(
          udf,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e,inputPutDataNull,inputPutDataType))
    }
  }

}
