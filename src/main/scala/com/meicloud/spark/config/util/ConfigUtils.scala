package com.meicloud.spark.config.util


import java.util

import com.meicloud.spark.entity.CaseVo.{JobConfigVo, OutputFileConfigVo, _}
import com.meicloud.spark.utils.{MySqlDBConnPoolUtils, StringUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by yesk on 2019-1-28. 
  */
object ConfigUtils {

  def getStaticInputJdbcConfig(mysqlReId: String): util.ArrayList[JdbcStaticConfVo] = {
    val conn = MySqlDBConnPoolUtils.getConn()
    val rsList = new util.ArrayList[JdbcStaticConfVo]()
    val sql =
      """
       select
          jd.jdbc_driver,
          jd.jdbc_url,
          jd.jdbc_user,
          jd.jdbc_pass,
          jd.min_pool_size,
          jd.max_pool_size,
          jd.acquire_increment,
          jd.max_statements,
          ssr.source_type,
          re.jdbc_table,
          ssr.source_id,
          ssr.source_alias AS regist_table_name
          FROM
          	t_ss_static_source_re ssr
          	LEFT JOIN t_ss_jdbc_re re ON re.jdbc_re_id = ssr.source_id
          	LEFT JOIN t_ss_jdbc jd ON re.jdbc_id = jd.jdbc_id
            where ssr.source_id is not null
            and  ssr.source_type = 'jdbc'
            and static_source_id = ?
      """
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, mysqlReId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow
    rs.beforeFirst()
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      while (rs.next()) {
        val jdbcStaticConfVo = new JdbcStaticConfVo(
          rs.getString("source_id"),
          rs.getString("source_type"),
          rs.getString("jdbc_driver"),
          rs.getString("jdbc_url"),
          rs.getString("jdbc_user"),
          rs.getString("jdbc_pass"),
          rs.getString("jdbc_table"),
          rs.getString("regist_table_name"),
          rs.getInt("min_pool_size"),
          rs.getInt("max_pool_size"),
          rs.getInt("acquire_increment"),
          rs.getInt("max_statements")
        )
        rsList.add(jdbcStaticConfVo)
      }
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
    }
    rsList
  }

  def getNodeConfigVo(jobName: String): util.ArrayList[(String, String, String)] = {
    val list: util.ArrayList[(String, String, String)] = new util.ArrayList[(String, String, String)]
    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """SELECT * from (select
                  node_type,
                  source_type,
                  source_or_exe_id as source_id
                  from t_ss_job job
                  JOIN t_ss_job_node node on job.job_id=node.job_id
                  where node_type='input' and job.job_name=? limit 1) input
                  UNION ALL
                  SELECT * from (select
                  node_type,
                  source_type,
                  source_or_exe_id as source_id
                  from t_ss_job job
                  JOIN t_ss_job_node node on job.job_id=node.job_id
                  where node_type='execute' and job.job_name=? limit 1) exe
                  UNION ALL
                  select
                  node_type,
                  source_type,
                  source_or_exe_id as source_id
                  from t_ss_job job
                  JOIN t_ss_job_node node on job.job_id=node.job_id
                  where node_type='output' and job.job_name=?
                  UNION ALL
                  select
                  node_type,
                  source_type,
                  source_or_exe_id as source_id
                  from t_ss_job job
                  JOIN t_ss_job_node node on job.job_id=node.job_id
                  where node_type='staticinput' and job.job_name=?
           """
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, jobName)
    pstm.setString(2, jobName)
    pstm.setString(3, jobName)
    pstm.setString(4, jobName)
    val rs = pstm.executeQuery()
    while (rs.next()) {
      list.add((rs.getString("node_type"), rs.getString("source_type"), rs.getString("source_id")))
    }
    rs.close()
    pstm.close()
    MySqlDBConnPoolUtils.releaseCon(conn)
    list
  }

  // 输入源kafka
  def getInputKafkaConfig(sKafkaId: String): InputKafkaConfigVo = {
    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ select
                      kre.s_kafka_id as source_id,
                      'kafka' as source_type,
                      ka.kafka_servers,
                      kre.subscribe_type,
                      kre.subscribe_content,
                      kre.starting_offsets,
                      kre.ending_offsets,
                      kre.max_offsets_pertrigger,
                      kre.data_type,
                      kre.data_schema,
                      kre.text_split
                    from t_ss_kafka_re kre
                    join t_ss_kafka ka ON  kre.f_kafka_id=ka.kafka_id
                    where kre.s_kafka_id=? """
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, sKafkaId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val inputKafkaConfigVo = new InputKafkaConfigVo(
        rs.getString("source_id"),
        rs.getString("source_type"),
        rs.getString("kafka_servers"),
        rs.getString("subscribe_type"),
        rs.getString("subscribe_content"),
        rs.getString("starting_offsets"),
        rs.getString("ending_offsets"),
        rs.getInt("max_offsets_pertrigger"),
        rs.getString("data_type"),
        rs.getString("data_schema"),
        rs.getString("text_split")
      )
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      inputKafkaConfigVo
    }
  }

  //输出源es
  def getOutputEsConfig(sEsId: String): OutputEsConfigVo = {

    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ SELECT  ere.s_es_id source_id,
                				'es' as source_type,
                				es.es_nodes,
                				es.es_port,
                        ere.es_resouce,
                				es.es_user,
                        es.es_passwd

                FROM  t_ss_es_re ere
                JOIN  t_ss_es es on ere.f_es_id=es.es_id
                where ere.s_es_id=? """

    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, sEsId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val outputEsConfigVo = new OutputEsConfigVo(
        rs.getString("source_id"),
        rs.getString("source_type"),
        rs.getString("es_nodes"),
        rs.getString("es_port"),
        rs.getString("es_resouce"),
        rs.getString("es_user"),
        rs.getString("es_passwd")
      )
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      outputEsConfigVo
    }
  }

  //执行sql
  def getExecuteSqlConfig(exeId: String): ExeSqlConfigVo = {

    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ SELECT exe.sql_id as exe_id,
                				'sql' as exe_type,
                				exe.sql_content as sql_content,
                        exe.data_primary,
                				group_concat(CONCAT_WS('::',udf.jar_path, udf.class_path, udf.method,re.is_nestcol, udf.udf_schema) separator '##') as udf_vo_list
                FROM   t_ss_sql exe
                JOIN t_ss_sql_udf_re re on re.sql_id=exe.sql_id
                JOIN t_ss_udf udf on re.udf_id=udf.udf_id
                WHERE exe.sql_id=? """
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, exeId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val udfList = ArrayBuffer[UDFConfigVo]()
      val executorId = rs.getString("exe_id")
      val executorType = rs.getString("exe_type")
      val dataPrimary = rs.getString("data_primary")
      val sqlContent = rs.getString("sql_content")
      val udfListStr = rs.getString("udf_vo_list")

      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      // 多个UDF函数
      if (null != udfListStr && !"".equals(udfListStr)) {
        val udfCombArr: Array[String] = udfListStr.split("##")
        for (udfComb <- udfCombArr) {
          val udfArr = udfComb.split("::")
          if (udfArr.size == 5 && !StringUtils.hasEmptyElement(udfArr))
            udfList.append(UDFConfigVo(udfArr(0), udfArr(1), udfArr(2), udfArr(3), udfArr(4)))
        }
        //只要有一个UDF函数设置失败就返回空
        if (udfList.size == udfCombArr.size) ExeSqlConfigVo(executorId, executorType, sqlContent, dataPrimary, udfList) else null
      } else {

        ExeSqlConfigVo(executorId, executorType, sqlContent, dataPrimary, udfList)
      }
    }
  }

  def updateJobConfigStatus(jobConfigVo: JobConfigVo): Unit = {
    val conn = MySqlDBConnPoolUtils.getConn()
    val sql =
      """ UPDATE t_ss_job set t_ss_job.status = 1 where job_name = ? """
    try {
      val pstm = conn.prepareStatement(sql)
      pstm.setString(1, jobConfigVo.jobName)
      pstm.addBatch()
      pstm.executeBatch()
    } finally {
      conn.close()
    }


  }

  def getJobConfigVo(jobName: String): JobConfigVo = {
    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ SELECT job_id, job_name, version, trade_name,update_time,
                          frequency, output_mode, fault_tolerant,
                          master
                  FROM t_ss_job
                 WHERE status in (0,3) and job_name = ?  """
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, jobName)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {

      val jobConfigVo = JobConfigVo(rs.getString("job_id"), rs.getString("job_name"),
        rs.getInt("version"), rs.getString("trade_name"), rs.getString("update_time"),
        rs.getInt("frequency"), rs.getString("output_mode"),
        rs.getString("fault_tolerant"), rs.getString("master"))
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      jobConfigVo
    }
  }

  def getJobAndInputConfigVo(jobName: String): (String, String) = {
    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ SELECT job.engine,node.source_type
                  FROM t_ss_job job join t_ss_job_node node on (job.job_id=node.job_id)
                 WHERE job.status in (0,1,2,3,4,41) and job_name = ? and node.node_type='input' """
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, jobName)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val engine = rs.getString("engine")
      val sourceType = rs.getString("source_type")
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      (engine, sourceType)
    }
  }

  //输出源JDBC
  def getOutputJdbcConfig(mysqlReId: String): OutputJdbcConfigVo = {

    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ select re.jdbc_re_id as source_id,
                         'jdbc' as source_type ,
                         jdbc_driver,
                         jd.jdbc_url,
                         jd.jdbc_user,
                         jd.jdbc_pass,
                         re.jdbc_table,
                         jd.min_pool_size,
                         jd.max_pool_size,
                         jd.acquire_increment,
                         jd.max_statements
                 from  t_ss_jdbc_re re  JOIN
                        t_ss_jdbc  jd
                        on re.jdbc_id=jd.jdbc_id
                 where re.jdbc_re_id=? """

    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, mysqlReId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val outputMysqlConfigVo = new OutputJdbcConfigVo(
        rs.getString("source_id"),
        rs.getString("source_type"),
        rs.getString("jdbc_driver"),
        rs.getString("jdbc_url"),
        rs.getString("jdbc_user"),
        rs.getString("jdbc_pass"),
        rs.getString("jdbc_table"),
        rs.getInt("min_pool_size"),
        rs.getInt("max_pool_size"),
        rs.getInt("acquire_increment"),
        rs.getInt("max_statements")
      )
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      outputMysqlConfigVo
    }
  }

  //输出源文件file
  def getOutputFileConfig(fileReId: String): OutputFileConfigVo = {

    val conn = MySqlDBConnPoolUtils.getConn();
    val sql =
      """ select file_id as source_id,
                          'file' as source_type ,
                           file_format,
                           file_path
                 from  t_ss_file_re re
                 where re.file_id=? """

    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, fileReId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow;
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val outputFileConfigVo = new OutputFileConfigVo(
        rs.getString("source_id"),
        rs.getString("source_type"),
        rs.getString("file_format"),
        rs.getString("file_path")
      )
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      outputFileConfigVo
    }
  }

  //输出源为kafka
  def getOutputKafkaConfig(kafkaReId: String): OutputKafkaConfigVo = {
    val conn = MySqlDBConnPoolUtils.getConn()
    val sql =
      """ SELECT re.s_kafka_id as source_id,
              re.data_type as source_type,
              re.subscribe_content as kafka_topic,
              s.kafka_servers as kafka_servers,
              'kafka' as kafka_format
              from t_ss_kafka_re  re left join t_ss_kafka  s
               on re.f_kafka_id = s.kafka_id where re.s_kafka_id =? """

    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, kafkaReId)
    val rs = pstm.executeQuery()
    rs.last()
    val count = rs.getRow
    if (count < 1) {
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      null
    }
    else {
      val outputKafkaConfigVo = new OutputKafkaConfigVo(
        rs.getString("source_id"),
        rs.getString("source_type"),
        rs.getString("kafka_topic"),
        rs.getString("kafka_servers"),
        rs.getString("kafka_format")
      )
      rs.close()
      pstm.close()
      MySqlDBConnPoolUtils.releaseCon(conn)
      outputKafkaConfigVo
    }
  }


  def main(args: Array[String]): Unit = {
    //    System.out.println(getInputKafkaConfig("1"))
    //    System.out.println(getOutputEsConfig("1"))

    val rs = getStaticInputJdbcConfig("e8f47e11d46411e9bc3ba4dcbe0e8346")
    System.out.println(getExecuteSqlConfig("sql_1"))
    System.out.println(getJobAndInputConfigVo("kafka2es-car-01"))
  }

}
