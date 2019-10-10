package com.meicloud.spark.offset.redis

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

object RedisSeriaUtils {


  /**
   * 序列化方法
   * @param obj 序列化对象
   */
  def serializableObject(obj: Object): Array[Byte] = {
    var oos: ObjectOutputStream = null
    var baos: ByteArrayOutputStream = null
    try {
      // 序列化
      baos = new ByteArrayOutputStream()
      oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      val bytes: Array[Byte] = baos.toByteArray()
      bytes
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    null
  }


  /**
   * 反序列化对象方法
   * @param bytes object 数组
   */
  def unserializableObject(bytes: Array[Byte]): Object = {
    var bais: ByteArrayInputStream = null
    try {
      // 反序列化
      bais = new ByteArrayInputStream(bytes)
      val ois: ObjectInputStream = new ObjectInputStream(bais)
      ois.readObject();
    } catch {
      case e: Exception => {
        e.printStackTrace();
      }
    }
    null
  }

}
