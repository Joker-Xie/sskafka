package com.meicloud.spark.utils

import com.google.gson.Gson
import jdk.nashorn.internal.parser.JSONParser

import scala.collection.mutable
import scala.collection.JavaConversions.mapAsScalaMap
import scala.util.parsing.json.JSONObject
import scala.collection.mutable
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.util

import org.apache.spark.sql.Row

object JsonToMapUtils {


  def main(args: Array[String]): Unit = {
    println(jsonStrToMap("{\"name\":\"yesk\",\"city\":\"sd\"}"))
  }

  def jsonStrToMap(json: String): mutable.HashMap[String,String]  = toStrMap(parseJson(json))

  def parseJson(json: String): JsonObject  = {
    val parser = new JsonParser();
    val jsonObj = parser.parse(json).getAsJsonObject
    jsonObj
  }




  /**
    * 将JSONObjec对象转换成Map-List集合
    *
    * @param json
    * @return
    */
  def toMap(json: JsonObject ): mutable.HashMap[String,Object] = {
    val map = new mutable.HashMap[String, Object]();
    val entrySet = json.entrySet
    val iter = entrySet.iterator
    while ( {
      iter.hasNext
    }) {
      val entry = iter.next
      val key = entry.getKey
      val value = entry.getValue
      if (value.isInstanceOf[JsonArray]) map.put(key.asInstanceOf[String], toList(value.asInstanceOf[JsonArray]))
      else if (value.isInstanceOf[JsonObject]) map.put(key.asInstanceOf[String], toMap(value.asInstanceOf[JsonObject]))
      else map.put(key.asInstanceOf[String], value)
    }
    map
  }
  def toStrMap(json: JsonObject ): mutable.HashMap[String,String] = {
    val map = new mutable.HashMap[String, String]();
    val entrySet = json.entrySet
    val iter = entrySet.iterator
    while ( {
      iter.hasNext
    }) {
      val entry = iter.next
      val key = entry.getKey
      val value = entry.getValue
      if (value.isInstanceOf[JsonArray]) map.put(key.asInstanceOf[String], toList(value.asInstanceOf[JsonArray]).toString)
      else if (value.isInstanceOf[JsonObject]) map.put(key.asInstanceOf[String], toMap(value.asInstanceOf[JsonObject]).toString())
      else map.put(key.asInstanceOf[String], value.toString)
    }
    map
  }
  /**
    * 将JSONArray对象转换成List集合
    *
    * @param json
    * @return
    */
  def toList(json: JsonArray): util.List[Object] = {
    val list = new util.ArrayList[Object]
    var i = 0
    while ( {
      i < json.size
    }) {
      val value = json.get(i)
      if (value.isInstanceOf[JsonArray]) list.add(toList(value.asInstanceOf[JsonArray]))
      else if (value.isInstanceOf[JsonObject]) list.add(toMap(value.asInstanceOf[JsonObject]))
      else list.add(value)

      {
        i += 1; i - 1
      }
    }
    list
  }
}
