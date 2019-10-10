package com.meicloud.spark.utils
import java.util.LinkedList
import java.sql.DriverManager
import java.sql.Connection
/**
  * Created by yesk on 2019-1-28. 
  */
object MySqlDBConnPoolUtils {
  private val max_connection = PropertiesScalaUtils.getString("mysql.max_connection") //连接池总数
  private val connection_num = PropertiesScalaUtils.getString("mysql.connection_num") //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = PropertiesScalaUtils.getString("mysql.driver")
  private val url = PropertiesScalaUtils.getString("mysql.url")
  private val username = PropertiesScalaUtils.getString("mysql.user")
  private val password = PropertiesScalaUtils.getString("mysql.password")
  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(driver)
    }
  }
  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(url, username, password)
    conn
  }
  /**
    * 初始化连接池
    */
  private def initConnectionPool(): LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
    * 获得连接
    */
  def getConn():Connection={
    initConnectionPool()
    pools.poll()
  }
  /**
    * 释放连接
    */
  def releaseCon(con:Connection){
    pools.push(con)
  }

}
