package com.meicloud.spark.utils

import java.util.Properties

/**
  * Created by yesk on 2019-1-28. 
  */
object PropertiesScalaUtils {
  val properties = new Properties()
  val in = PropertiesScalaUtils.getClass.getClassLoader.getResourceAsStream("config.properties")
  //val path = Thread.currentThread().getContextClassLoader.getResource("config_scala.properties").getPath //文件要放到resource文件夹下
  properties.load(in)
  in.close()
  def main(args: Array[String]): Unit = {
    System.out.println(loadProperties("mysql.driver"))
  }
  def loadProperties(key:String):String = {
    properties.getProperty(key)
  }
  def getString(key:String):String = {
    properties.getProperty(key)
  }
}
