package com.meicloud.spark.utils

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.apache.spark.sql.types._
import org.json4s.jackson.Json

object SchemaUtil {
  def main(args: Array[String]): Unit = {
    val schema = StructType(
      StructField("name", StringType) ::
        StructField("age", IntegerType) ::
        StructField("score", ArrayType(ShortType, true)) ::
        StructField("params", StructType(Array(StructField("extra", StringType)))) ::
        StructField("goal", MapType(StringType, DoubleType, true)) :: Nil)

    val str = schema2str(schema)
    val schema2 = str2schema(str)
//    println(str)
//    println(schema2)
    val schemaStr="{\"name\":\"string\",\"age\":\"integer\",\"params\":{\"extra\":\"string\"}}"
    println(str2schema(schemaStr))

    val schema1 = new StructType().add("name",StringType).add("age",IntegerType)
      .add("params",new StructType().add("extra",StringType))
    println(schema1)
    //{"name":"string","age":"integer","score":["short"],"params":{"extra":"string"},"goal":["string","double"]}
    //StructType(StructField(name,StringType,true), StructField(age,IntegerType,true), StructField(score,ArrayType(ShortType,true),true), StructField(params,StructType(StructField(extra,StringType,true)),true), StructField(goal,MapType(StringType,DoubleType,true),true))
  }

  /*
   * 将schema转为json字符串，有利于保存
   */
  def schema2str(schema: StructType) = {
    val jobj = datatype2jvalue(schema)
    compact(render(jobj))
  }

  /*
   * 将json字符串转为schema
   */
  def str2schema(str: String):StructType = {
    val jvalue = parse(str)
    require(jvalue.isInstanceOf[JObject], s"Type must be JObject, but ${jvalue} found.")
    jvalue2datatype(jvalue).asInstanceOf[StructType]
  }

  private[this] def datatype2jvalue(dt: DataType): JValue = {
    dt match {
      case st: StructType =>
        val r = st.map {
          case StructField(name, dataType, _, _) =>
            val json: JObject = (name -> datatype2jvalue(dataType))
            json
        }
        r.reduce(_ ~ _)

      case at: ArrayType =>
        JArray(List(datatype2jvalue(at.elementType)))

      case mt: MapType =>
        JArray(List(datatype2jvalue(mt.keyType), datatype2jvalue(mt.valueType)))

      case _ => JString(dt.typeName)
    }
  }

  private[this] val typeMap = Map[String, DataType](
    "string" -> org.apache.spark.sql.types.StringType,
    "short" -> org.apache.spark.sql.types.ShortType,
    "integer" -> org.apache.spark.sql.types.IntegerType,
    "long" -> org.apache.spark.sql.types.LongType,
    "float" -> org.apache.spark.sql.types.FloatType,
    "double" -> org.apache.spark.sql.types.DoubleType,
    "boolean" -> org.apache.spark.sql.types.BooleanType,
    "byte" -> org.apache.spark.sql.types.ByteType,
    "binary" -> org.apache.spark.sql.types.BinaryType,
    "date" -> org.apache.spark.sql.types.DateType,
    "timestamp" -> org.apache.spark.sql.types.TimestampType,
    "calendarinterval" -> org.apache.spark.sql.types.CalendarIntervalType,
    "null" -> org.apache.spark.sql.types.NullType)

  private[this] def jvalue2datatype(jdt: JValue): DataType = {
    jdt match {
      case js: JString =>
        val type_str = js.s
        val res = typeMap.get(type_str)

        if (res == None) {
          require(type_str.startsWith("decimal"), s"Type ${type_str} unknow.")
          val regex = """decimal\((\d+),(\d+)\)""".r
          val regex(precision, scale) = type_str
          org.apache.spark.sql.types.DecimalType(precision.toInt, scale.toInt)
        } else {
          res.get
        }

      case ja: JArray =>
        if (ja.values.size == 1) {
          ArrayType(jvalue2datatype(ja.arr(0)), true)
        } else {
          val keyType = jvalue2datatype(ja.arr(0))
          val valueType = jvalue2datatype(ja.arr(1))
          MapType(keyType, valueType, true)
        }

      case jo: JObject =>
        val jf = jo.obj
        val sfs = jf.map {
          case (name: String, ctpye: JValue) =>
            StructField(name, jvalue2datatype(ctpye))
        }
        StructType(sfs)
      case other: Any =>
        throw new RuntimeException(s"Not JObject/JArray/JString, type:${other.getClass}")

    }

  }
}
