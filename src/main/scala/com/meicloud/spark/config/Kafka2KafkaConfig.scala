package com.meicloud.spark.config

import com.meicloud.spark.entity.CaseVo.Kafka2KafkaConfigVo
import com.meicloud.spark.utils.MySqlDBConnPoolUtils

/**
  * Created by yesk on 2019-1-28. 
  */
object Kafka2KafkaConfig {
  def main(args: Array[String]): Unit = {
    System.out.println(getConfig("kafka2es-car-002"))
  }

  def getConfig(configId:String):Kafka2KafkaConfigVo={
      val conn = MySqlDBConnPoolUtils.getConn();
      val sql = new StringBuilder()
          .append("SELECT config_id, trade_name,kafka_bootstrap_servers,subscribe_type,")
          .append("subscribe_content,starting_offsets,ending_offsets,max_offsetspertrigger,")
          .append("data_type,data_schema,output_mode,")
          .append("out_kafka_bootstrap_servers,out_topic,out_trigger")
      .append("  FROM t_kafka2kafka")
      .append(" WHERE config_id = ?")
      val pstm = conn.prepareStatement(sql.toString())
      pstm.setString(1,configId)
      val rs = pstm.executeQuery()
      rs.last()
      val count = rs.getRow;
      if(count<1) {
        rs.close()
        pstm.close()
        MySqlDBConnPoolUtils.releaseCon(conn)
        null
      }else{
        val kafka2Kafka = new Kafka2KafkaConfigVo(
          rs.getString("config_id"),rs.getString("trade_name"),
          rs.getString("kafka_bootstrap_servers"), rs.getString("subscribe_type"),
          rs.getString("subscribe_content"),rs.getString("starting_offsets"),
          rs.getString("ending_offsets"),rs.getInt("max_offsetspertrigger"),
          rs.getString("data_type"),rs.getString("data_schema"),
          rs.getString("output_mode"),rs.getString("out_kafka_bootstrap_servers"),
          rs.getString("out_topic"),rs.getString("out_trigger"))

        rs.close()
        pstm.close()
        MySqlDBConnPoolUtils.releaseCon(conn)
        kafka2Kafka
      }
  }



}
