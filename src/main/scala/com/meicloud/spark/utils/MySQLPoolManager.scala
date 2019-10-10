package com.meicloud.spark.utils
import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource
import com.meicloud.spark.entity.CaseVo.OutputJdbcConfigVo
/**
  * Created by yesk on 2019-3-25. 
  */
object MySQLPoolManager {
  var mysqlManager:MysqlPool = _

  def getMysqlManager(outputJdbcConfigVo: OutputJdbcConfigVo):MysqlPool = {
    synchronized {
      if(mysqlManager == null){
        mysqlManager = new MysqlPool(outputJdbcConfigVo)
      }
    }
    mysqlManager
  }

  class MysqlPool(outputJdbcConfigVo: OutputJdbcConfigVo) extends Serializable {
    private val druid:DruidDataSource = new DruidDataSource(true)
    try {
      druid.setUrl(outputJdbcConfigVo.jdbcUrl)
      druid.setDriverClassName(outputJdbcConfigVo.jdbcDriver)
      druid.setUsername(outputJdbcConfigVo.jdbcUser)
      druid.setPassword(outputJdbcConfigVo.jdbcPass)
      druid.setMinIdle(outputJdbcConfigVo.minPoolSize)
      druid.setMaxActive(outputJdbcConfigVo.maxPoolSize)
    } catch {
      case e:Exception => e.printStackTrace( )
    }

    def getConnection:Connection = {
      try {
        druid.getConnection()
      } catch {
        case ex:Exception =>
          ex.printStackTrace()
        null
      }
    }

    def close():Unit = {
      try {
        druid.close()
      } catch {
        case ex:Exception =>
          ex.printStackTrace()
      }
    }
  }
}
