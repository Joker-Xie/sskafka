package com.meicloud.spark.offset.redis

import java.util

import org.apache.spark.sql.Row

object RedisOffsetObj extends Serializable {
  val offsetObj = new util.HashMap[String, Map[String, Map[String, Long]]]()

  def put(record: Row): Unit = {
    offsetObj.put(record.getString(0),
      Map[String, Map[String, Long]](record.getString(0) ->
        Map(record.getInt(1).toString -> record.getLong(2)))
    )
  }

  def get(obj: String): String = {
    offsetObj.get(obj).toString()
  }
}
