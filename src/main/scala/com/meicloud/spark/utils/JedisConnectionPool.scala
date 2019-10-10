package com.meicloud.spark.utils

import com.meicloud.spark.utils.PropertiesScalaUtils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool extends Serializable {
  //连接配置
  val config = new JedisPoolConfig
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  val ip = PropertiesScalaUtils.getString("redis.ip")
  val port = PropertiesScalaUtils.getString("redis.port").toInt
  val passport = PropertiesScalaUtils.getString("redis.pass")
  val timeout = PropertiesScalaUtils.getString("redis.timeout").toInt
  //设置连接池属性分别有： 配置  主机名   端口号  连接超时时间    Redis密码
  val pool = new JedisPool(config,ip , port, timeout,passport)
//  val pool = new JedisPool(config,"127.0.0.1" , 6379, 1000)

  //连接池
  def getConnections(): Jedis = {
    pool.getResource
  }
  def rebackPool(jedis:Jedis): Unit ={
    pool.returnResource(jedis)
  }
}
