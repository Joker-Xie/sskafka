package com.meicloud.flink.stream.utils;

import com.meicloud.flink.asynstream.JdbcAsynStream;
import com.meicloud.spark.entity.CaseVo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class MysqlUtilsTest {

    @Test
    public void testGetDBMetaInfo() {
    }

    @Test
    public void jdbcStaticDataLoad() {
        CaseVo.JdbcStaticConfVo jdbcStaticConfVo = new CaseVo.JdbcStaticConfVo("", "", "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306?characterEncoding=UTF-8"
                , "root", "123456", "mydb.t_category", "t_category", 2, 10, 1, 10);
        Cache<String, ResultSet> cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(2, TimeUnit.SECONDS)
                .build();
        JdbcAsynStream jdbcAsynStream = new JdbcAsynStream(jdbcStaticConfVo, cache);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings evSettings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment evTable = StreamTableEnvironment.create(env, evSettings);
        DataStream<String> source = env.fromElements("1");
        SingleOutputStreamOperator<Row> dataStream = AsyncDataStream.orderedWait(source, jdbcAsynStream, 2, TimeUnit.MINUTES);
        evTable.registerDataStream("testTable", dataStream, "category_id,category,org_code,lowest_price,highest_price,online_flag,order_no,unit_lowest_price,unit_highest_price,sale_unit,online_date,man_thr,cut_thr");
        Table table = evTable.sqlQuery("select category_id,category,org_code,lowest_price,highest_price,online_flag,order_no,unit_lowest_price,unit_highest_price,sale_unit,online_date,man_thr,cut_thr from testTable");
//        Table table = evTable.sqlQuery("select category_id from testTable");
        evTable.
                toRetractStream(table, Types.ROW_NAMED("category_id,category,org_code,lowest_price,highest_price,online_flag,order_no,unit_lowest_price,unit_highest_price,sale_unit,online_date,man_thr,cut_thr".split(","),
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.FLOAT,
                Types.FLOAT,
                Types.INT,
                Types.VOID,
                Types.FLOAT,
                Types.FLOAT,
                Types.STRING,
                Types.SQL_TIMESTAMP,
                Types.FLOAT,
                Types.FLOAT
        )).print();
        try {
            env.execute("Start Stream");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}