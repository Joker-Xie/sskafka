package com.meicloud.spark.error

import java.util

import com.meicloud.spark.error.StreamErrorDataConsumer.props
import com.meicloud.spark.offset.redis.RedisOffsetOperate
import com.meicloud.spark.utils.MySqlDBConnPoolUtils
import org.apache.kafka.clients.consumer.KafkaConsumer

object Regular4ErrorData {

  /**
   * 计算异常数据的总条数
   * @param jobname
   * @param topic
   */
  def getErrorDataNum(jobname: String, topic: String): Long = {
    val consumer = new KafkaConsumer[String, String](props)
    val offsetObj = RedisOffsetOperate.getOffsetMsg2Kafka(jobname, topic + "_error_data", consumer)
    val topics = new util.ArrayList[String]()
    topics.add(topic)
    val topicEndOffset = consumer.endOffsets(offsetObj._1)
    //设置kafka各分区开始的消费offset
    val itor = offsetObj._2.keySet().iterator()
    var errorNum = 0L
    while (itor.hasNext) {
      val key = itor.next()
      val startOffset = offsetObj._2.get(key)
      val endOffset = topicEndOffset.get(key)
      errorNum += (endOffset - startOffset)
    }
    errorNum
  }

  /**
    * 用于监控异常数据条数
    *
    * @param jobName
    * @param errorDataNum
    */
  def regularWatcher(jobName: String, errorDataNum: Long): Unit = {
    val connt = MySqlDBConnPoolUtils.getConn()
    try {
      val sql = String.format("INSERT INTO t_ss_job (job_id,job_name,err_data_num,transform_err_data_num) VALUES (0,'%s','%s',0)  ON DUPLICATE KEY UPDATE err_data_num='%s';"
        , jobName, errorDataNum.toString, errorDataNum.toString
      )
      val ppst = connt.prepareStatement(sql)
      ppst.execute()
    } finally {
      connt.close()
    }
  }

  /**
   * 用于更新某批次将处理多少的异常数据返回源kafka的topic
   * @param jobName
   * @param errorDataNum
   */
  def updateErroNum(jobName: String, errorDataNum: Long): Unit ={
    val connt = MySqlDBConnPoolUtils.getConn()
    try {
      val sql = String.format("INSERT INTO t_ss_job (job_id,job_name,err_data_num,transform_err_data_num) VALUES (0,'%s',0,0)  ON DUPLICATE KEY UPDATE transform_err_data_num='%s';"
        , jobName, errorDataNum.toString
      )
      val ppst = connt.prepareStatement(sql)
      ppst.execute()
    } finally {
      connt.close()
    }

  }

  def main(args: Array[String]): Unit = {
    val testStr = String.format("hello: %s ", "xiaowang")
    val testNum = getErrorDataNum("kafa2mysql-white-03", "gxt.comment.white.redo.test")
    regularWatcher("kafa2mysql-white-03", testNum)
    println("xxxxxx")
  }
}
