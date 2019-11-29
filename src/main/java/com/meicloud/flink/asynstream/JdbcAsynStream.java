package com.meicloud.flink.asynstream;

import com.meicloud.flink.stream.utils.MysqlUtils;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * @author xiehy11
 * <p>
 * 2019/11/07
 */

public class JdbcAsynStream implements AsyncFunction<String, Row>, ResultTypeQueryable {
    private JdbcStaticConfVo jdbcStaticConfVo;
    private Cache<String, ResultSet> cache;
    private static Logger logger = Logger.getLogger(JdbcAsynStream.class);

    public JdbcAsynStream(JdbcStaticConfVo jdbcStaticConfVo,Cache<String, ResultSet> cache) {
        this.jdbcStaticConfVo = jdbcStaticConfVo;
        this.cache = cache;
    }

    @SuppressWarnings("AlibabaThreadPoolCreation")
    @Override
    public void asyncInvoke(String input, ResultFuture<Row> resultFuture) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);
        String tbName = jdbcStaticConfVo.jdbcTableName();
        String fetchDataSql = "select * from " + tbName;
        MysqlUtils mysqlObj = new MysqlUtils(jdbcStaticConfVo);
        Connection conn = mysqlObj.getJdbcConnt();
        PreparedStatement pst = conn.prepareStatement(fetchDataSql);
        ListenableFuture<ResultSet> lf = listeningExecutor.submit(new Callable<ResultSet>() {
            @Override
            public ResultSet call() throws Exception {
                ResultSet rs = null;
                if (cache.getIfPresent(tbName) == null) {
                    rs = pst.executeQuery();
                    cache.put(tbName, rs);
                }
                rs = cache.getIfPresent(tbName);
                List<Row> tableSet = resultSet2CollectionsOfRow(rs);
                resultFuture.complete(tableSet);
                Thread.sleep(3000);
                System.out.println("task finished!");
                logger.info("Dim Table Load: " + tbName);
                return rs;
            }
        });
    }


    @Override
    public TypeInformation getProducedType() {
        //TODO 优化可考虑使用缓存
        RowTypeInfo rowTypeInfo = null;
        MysqlUtils mysqlObj = new MysqlUtils(jdbcStaticConfVo);
        Connection conn = mysqlObj.getJdbcConnt();
        String tableName = jdbcStaticConfVo.jdbcTableName();
        String sql = "select * from " + tableName;
        ResultSetMetaData resultMetaInfo = null;
        try {
            PreparedStatement pst = conn.prepareStatement(sql);
            resultMetaInfo = pst.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            int colums = resultMetaInfo.getColumnCount();
            TypeInformation[] typeInfos = new TypeInformation[colums];
            for (int i = 0; i < colums; i++) {
                String columTypeName = resultMetaInfo.getColumnTypeName(i + 1);
                typeInfos[i] = MysqlUtils.dbTypeTransfromRowType(columTypeName);
            }
            conn.close();
            rowTypeInfo = new RowTypeInfo(typeInfos);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rowTypeInfo;
    }

    /**
     * 将一个数据库的数据集转换成一个Row的集合
     *
     * @param rs
     */
    private List<Row> resultSet2CollectionsOfRow(ResultSet rs) throws SQLException {
        List<Row> rowList = new ArrayList();
        ResultSetMetaData resultMetaInfo = rs.getMetaData();
        int colNums = resultMetaInfo.getColumnCount();
        while (rs.next()) {
            Row row = new Row(colNums);
            for (int i = 0; i < colNums; i++) {
                MysqlUtils.typeInfos2SqlType(resultMetaInfo.getColumnTypeName(i + 1), rs, i, row);
            }
            rowList.add(row);
        }
        return rowList;
    }

}


