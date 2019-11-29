package com.meicloud.flink.conf;

import com.meicloud.spark.entity.CaseVo.*;
import com.meicloud.spark.utils.MySqlDBConnPoolUtils;
import com.meicloud.spark.utils.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.collection.mutable.ArrayBuffer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ConfigUtils {
    public static ArrayList<JdbcStaticConfVo> getStaticInputJdbcConfig(String mysqlReId) {
        Connection conn = MySqlDBConnPoolUtils.getConn();
        ArrayList rsList = new ArrayList<JdbcStaticConfVo>();
        String sql = "select\n" +
                "        jd.jdbc_driver,\n" +
                "                jd.jdbc_url,\n" +
                "                jd.jdbc_user,\n" +
                "                jd.jdbc_pass,\n" +
                "                jd.min_pool_size,\n" +
                "                jd.max_pool_size,\n" +
                "                jd.acquire_increment,\n" +
                "                jd.max_statements,\n" +
                "                ssr.source_type,\n" +
                "                re.jdbc_table,\n" +
                "                ssr.source_id,\n" +
                "                ssr.source_alias AS regist_table_name\n" +
                "        FROM\n" +
                "        t_ss_static_source_re ssr\n" +
                "        LEFT JOIN t_ss_jdbc_re re ON re.jdbc_re_id = ssr.source_id\n" +
                "        LEFT JOIN t_ss_jdbc jd ON re.jdbc_id = jd.jdbc_id\n" +
                "        where ssr.source_id is not null\n" +
                "        and  ssr.source_type = 'jdbc'\n" +
                "        and static_source_id = ?";
        PreparedStatement pstm = null;
        try {
            pstm = conn.prepareStatement(sql.toString());
            pstm.setString(1, mysqlReId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            rs.beforeFirst();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                while (rs.next()) {
                    JdbcStaticConfVo jdbcStaticConfVo = new JdbcStaticConfVo(
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
                    );
                    rsList.add(jdbcStaticConfVo);
                }
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rsList;
    }

    public static ArrayList<Tuple3<String, String, String>> getNodeConfigVo(String jobName) {
        ArrayList<Tuple3<String, String, String>> list = new ArrayList<Tuple3<String, String, String>>();
        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "SELECT * from (select\n" +
                "        node_type,\n" +
                "                source_type,\n" +
                "                source_or_exe_id as source_id\n" +
                "        from t_ss_job job\n" +
                "        JOIN t_ss_job_node node on job.job_id=node.job_id\n" +
                "        where node_type='input' and job.job_name=? limit 1) input\n" +
                "        UNION ALL\n" +
                "        SELECT * from (select\n" +
                "                node_type,\n" +
                "                source_type,\n" +
                "                source_or_exe_id as source_id\n" +
                "                from t_ss_job job\n" +
                "                JOIN t_ss_job_node node on job.job_id=node.job_id\n" +
                "                where node_type='execute' and job.job_name=? limit 1) exe\n" +
                "        UNION ALL\n" +
                "        select\n" +
                "                node_type,\n" +
                "                source_type,\n" +
                "        source_or_exe_id as source_id\n" +
                "        from t_ss_job job\n" +
                "        JOIN t_ss_job_node node on job.job_id=node.job_id\n" +
                "        where node_type='output' and job.job_name=?\n" +
                "        UNION ALL\n" +
                "        select\n" +
                "                node_type,\n" +
                "                source_type,\n" +
                "        source_or_exe_id as source_id\n" +
                "        from t_ss_job job\n" +
                "        JOIN t_ss_job_node node on job.job_id=node.job_id\n" +
                "        where node_type='staticinput' and job.job_name=?";
        PreparedStatement pstm = null;

        try {
            pstm = conn.prepareStatement(sql);
            pstm.setString(1, jobName);
            pstm.setString(2, jobName);
            pstm.setString(3, jobName);
            pstm.setString(4, jobName);
            ResultSet rs = pstm.executeQuery();
            while (rs.next()) {
                list.add(new Tuple3(rs.getString("node_type"), rs.getString("source_type"), rs.getString("source_id")));
            }
            rs.close();
            pstm.close();
            MySqlDBConnPoolUtils.releaseCon(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    // 输入源kafka
    public static InputKafkaConfigVo getInputKafkaConfig(String sKafkaId) {
        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "select\n" +
                "        kre.s_kafka_id as source_id,\n" +
                "                'kafka' as source_type,\n" +
                "        ka.kafka_servers,\n" +
                "                kre.subscribe_type,\n" +
                "                kre.subscribe_content,\n" +
                "                kre.starting_offsets,\n" +
                "                kre.ending_offsets,\n" +
                "                kre.max_offsets_pertrigger,\n" +
                "                kre.data_type,\n" +
                "                kre.data_schema,\n" +
                "                kre.text_split\n" +
                "        from t_ss_kafka_re kre\n" +
                "        join t_ss_kafka ka ON  kre.f_kafka_id=ka.kafka_id\n" +
                "        where kre.s_kafka_id=?";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql.toString());
            pstm.setString(1, sKafkaId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                InputKafkaConfigVo inputKafkaConfigVo = new InputKafkaConfigVo(
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
                );
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return inputKafkaConfigVo;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //输出源es
    public static OutputEsConfigVo getOutputEsConfig(String sEsId) {

        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql =
                "SELECT  ere.s_es_id source_id,\n" +
                        "        'es' as source_type,\n" +
                        "        es.es_nodes,\n" +
                        "                es.es_port,\n" +
                        "                ere.es_resouce,\n" +
                        "                es.es_user,\n" +
                        "                es.es_passwd\n" +
                        "\n" +
                        "        FROM  t_ss_es_re ere\n" +
                        "        JOIN  t_ss_es es on ere.f_es_id=es.es_id\n" +
                        "        where ere.s_es_id=?";

        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, sEsId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                OutputEsConfigVo outputEsConfigVo = new OutputEsConfigVo(
                        rs.getString("source_id"),
                        rs.getString("source_type"),
                        rs.getString("es_nodes"),
                        rs.getString("es_port"),
                        rs.getString("es_resouce"),
                        rs.getString("es_user"),
                        rs.getString("es_passwd")
                );
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return outputEsConfigVo;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    //执行sql
    public static JavaExeSqlConfigVo getExecuteSqlConfig(String exeId) {

        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "SELECT exe.sql_id as exe_id,\n" +
                "        'sql' as exe_type,\n" +
                "        exe.sql_content as sql_content,\n" +
                "            exe.data_primary,\n" +
                "            group_concat(CONCAT_WS('::', udf.jar_path, udf.class_path, udf.method, re.is_nestcol, udf.udf_schema)separator\n" +
                "        '##')as udf_vo_list\n" +
                "        FROM t_ss_sql exe\n" +
                "        JOIN t_ss_sql_udf_re re on re.sql_id = exe.sql_id\n" +
                "        JOIN t_ss_udf udf on re.udf_id = udf.udf_id\n" +
                "        WHERE exe.sql_id =?";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, exeId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                List<UDFConfigVo> udfList = new ArrayList();
                String executorId = rs.getString("exe_id");
                String executorType = rs.getString("exe_type");
                String dataPrimary = rs.getString("data_primary");
                String sqlContent = rs.getString("sql_content");
                String udfListStr = rs.getString("udf_vo_list");

                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
//                // 多个UDF函数
                if (null != udfListStr && !"".equals(udfListStr)) {
                    String[] udfCombArr = udfListStr.split("##");
                    for (String udfComb : udfCombArr) {
                        String[] udfArr = udfComb.split("::");
                        if (udfArr.length == 5 && !StringUtils.hasEmptyElement(udfArr)) {
                            UDFConfigVo udfConfigVo = new UDFConfigVo(udfArr[0], udfArr[1], udfArr[2], udfArr[3], udfArr[4]);
                            udfList.add(udfConfigVo);
                        }
                    }
                    //只要有一个UDF函数设置失败就返回空
                    if (udfList.size() == udfCombArr.length) {
                        return new JavaExeSqlConfigVo(executorId, executorType, sqlContent, dataPrimary, udfList);
                    }
                    return null;
                } else {
                    return new JavaExeSqlConfigVo(executorId, executorType, sqlContent, dataPrimary, udfList);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void updateJobConfigStatus(JobConfigVo jobConfigVo) {
        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "UPDATE t_ss_job set t_ss_job.status = 1 where job_name = ?";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, jobConfigVo.jobName());
            pstm.addBatch();
            pstm.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static JobConfigVo getJobConfigVo(String jobName) {
        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "SELECT job_id, job_name, version, trade_name,update_time,\n" +
                "        frequency, output_mode, fault_tolerant,\n" +
                "                master\n" +
                "        FROM t_ss_job\n" +
                "        WHERE status in(0, 3) and job_name = ?";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, jobName);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {

                JobConfigVo jobConfigVo = new JobConfigVo(rs.getString("job_id"), rs.getString("job_name"),
                        rs.getInt("version"), rs.getString("trade_name"), rs.getString("update_time"),
                        rs.getInt("frequency"), rs.getString("output_mode"),
                        rs.getString("fault_tolerant"), rs.getString("master"));
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return jobConfigVo;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Tuple2<String, String> getJobAndInputConfigVo(String jobName) {
        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = " SELECT job.engine,node.source_type\n" +
                "        FROM t_ss_job job join t_ss_job_node node on(job.job_id = node.job_id)\n" +
                "        WHERE job.status in (0, 1, 2, 3, 4, 41)and job_name = ?and node.node_type = 'input'";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, jobName);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                String engine = rs.getString("engine");
                String sourceType = rs.getString("source_type");
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return new Tuple2(engine, sourceType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //输出源JDBC
    public static OutputJdbcConfigVo getOutputJdbcConfig(String mysqlReId) {

        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "select re.jdbc_re_id as source_id,\n" +
                "        'jdbc' as source_type,\n" +
                "            jdbc_driver,\n" +
                "        jd.jdbc_url,\n" +
                "                jd.jdbc_user,\n" +
                "                jd.jdbc_pass,\n" +
                "                re.jdbc_table,\n" +
                "                jd.min_pool_size,\n" +
                "                jd.max_pool_size,\n" +
                "                jd.acquire_increment,\n" +
                "                jd.max_statements\n" +
                "        from t_ss_jdbc_re re JOIN\n" +
                "        t_ss_jdbc jd\n" +
                "        on re.jdbc_id = jd.jdbc_id\n" +
                "        where re.jdbc_re_id =?";

        PreparedStatement pstm = null;
        try {
            pstm = conn.prepareStatement(sql);
            pstm.setString(1, mysqlReId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                OutputJdbcConfigVo outputMysqlConfigVo = new OutputJdbcConfigVo(
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
                );
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return outputMysqlConfigVo;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //输出源文件file
    public static OutputFileConfigVo getOutputFileConfig(String fileReId) {

        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "select file_id as source_id,\n" +
                "        'file' as source_type,\n" +
                "            file_format,\n" +
                "            file_path\n" +
                "        from t_ss_file_re re\n" +
                "        where re.file_id =?";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, fileReId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                OutputFileConfigVo outputFileConfigVo = new OutputFileConfigVo(
                        rs.getString("source_id"),
                        rs.getString("source_type"),
                        rs.getString("file_format"),
                        rs.getString("file_path")
                );
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return outputFileConfigVo;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //输出源为kafka
    public static OutputKafkaConfigVo getOutputKafkaConfig(String kafkaReId) {
        Connection conn = MySqlDBConnPoolUtils.getConn();
        String sql = "SELECT re.s_kafka_id as source_id,\n" +
                "        re.data_type as source_type,\n" +
                "            re.subscribe_content as kafka_topic,\n" +
                "        s.kafka_servers as kafka_servers,\n" +
                "            'kafka' as kafka_format\n" +
                "        from t_ss_kafka_re re left join t_ss_kafka s\n" +
                "        on re.f_kafka_id = s.kafka_id where re.s_kafka_id =?";
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.setString(1, kafkaReId);
            ResultSet rs = pstm.executeQuery();
            rs.last();
            int count = rs.getRow();
            if (count < 1) {
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return null;
            } else {
                OutputKafkaConfigVo outputKafkaConfigVo = new OutputKafkaConfigVo(
                        rs.getString("source_id"),
                        rs.getString("source_type"),
                        rs.getString("kafka_topic"),
                        rs.getString("kafka_servers"),
                        rs.getString("kafka_format")
                );
                rs.close();
                pstm.close();
                MySqlDBConnPoolUtils.releaseCon(conn);
                return outputKafkaConfigVo;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        ArrayList<JdbcStaticConfVo> rs = getStaticInputJdbcConfig("e8f47e11d46411e9bc3ba4dcbe0e8346");
        System.out.println(getExecuteSqlConfig("sql_1"));
        System.out.println(getJobAndInputConfigVo("kafka2es-car-01"));
    }
}
