package com.meicloud.flink.stream.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xhy11
 */

public class MysqlUtils {

    private JdbcStaticConfVo jdbcStaticConfVo;

    public MysqlUtils(JdbcStaticConfVo jdbcStaticConfVo) {
        this.jdbcStaticConfVo = jdbcStaticConfVo;
    }

    /**
     * 获取MYSQL对应表的元信息
     *
     * @return 返回MYSQL对应表的元信息
     */
    public Tuple2<String, TypeInformation[]> getDBMetaInfo() {
        try {
            Connection conn = getJdbcConnt();
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String catalogName = jdbcStaticConfVo.jdbcTableName().split("\\.")[0];
            String tableName = jdbcStaticConfVo.jdbcTableName().split("\\.")[1];
            ResultSet rs = dbMetaData.getColumns(catalogName, null, tableName, "%");
            List<TypeInformation> filesTypeList = new ArrayList<TypeInformation>();
            List<String> filesNameList = new ArrayList<String>();
            while (rs.next()) {
                TypeInformation fileType = dbTypeTransfromRowType(rs.getString("TYPE_NAME"));
                filesTypeList.add(fileType);
                filesNameList.add(rs.getString("COLUMN_NAME"));
            }
            TypeInformation[] filesType = filesTypeList.toArray(new TypeInformation[]{});
            String filesName = filesNameList.toString().replace("]", "").replace("[", "");
            return new Tuple2<>(filesName, filesType);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 返回Mysql字段类型对应的FlinkSQL的数据类型;
     *
     * @param typeName 类型名称
     */
    public static TypeInformation dbTypeTransfromRowType(String typeName) {
        switch (typeName) {
            case "INT":
                return Types.INT;
            case "VARCHAR":
                return Types.STRING;
            case "BIGINT":
                return Types.LONG;
            case "DATE":
                return Types.SQL_DATE;
            case "FLOAT":
                return Types.FLOAT;
            case "DOUBLE":
                return Types.DOUBLE;
            case "TIMESTAMP":
                return Types.SQL_TIMESTAMP;
            case "BYTE":
                return Types.BYTE;
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "CHAR":
                return Types.CHAR;
            default:
                return Types.VOID;

        }

    }

    /**
     * 通过配置jdbcStaticConfVo使用druid获取链接
     */
    public Connection getJdbcConnt() {
        DruidDataSource druid = new DruidDataSource(true);
        try {
            druid.setUrl(jdbcStaticConfVo.jdbcUrl());
            druid.setDriverClassName(jdbcStaticConfVo.jdbcDriver());
            druid.setUsername(jdbcStaticConfVo.jdbcUser());
            druid.setPassword(jdbcStaticConfVo.jdbcPass());
            druid.setMinIdle(jdbcStaticConfVo.minPoolSize());
            druid.setMaxActive(jdbcStaticConfVo.maxPoolSize());
            return druid.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据数据库类型使用相对的getXXX获取数据并存入Row的相应位置中
     *
     * @param columnTypeName
     * @param rs
     * @param i
     * @param row
     */
    public static void typeInfos2SqlType(String columnTypeName, ResultSet rs, int i, Row row) throws SQLException {
        switch (columnTypeName) {
            case "INT":
                row.setField(i, rs.getInt(i+1));
                break;
            case "INTEGER":
                row.setField(i, rs.getInt(i+1));
                break;
            case "TINYINT":
                row.setField(i, rs.getInt(i+1));
                break;
            case "BIGINT":
                row.setField(i, rs.getLong(i+1));
                break;
            case "FLOAT":
                row.setField(i, rs.getFloat(i+1));
                break;
            case "DOUBLE":
                row.setField(i, rs.getDouble(i+1));
                break;
            case "VARCHAR":
                row.setField(i, rs.getString(i+1));
                break;
            case "CHAR":
                row.setField(i, rs.getString(i+1));
                break;
            case "TEXT":
                row.setField(i, rs.getString(i+1));
                break;
            case "DATE":
                row.setField(i, rs.getDate(i+1));
                break;
            case "TIME":
                row.setField(i, rs.getTime(i+1));
                break;
            case "TIMESTAMP":
                row.setField(i, rs.getTimestamp(i+1));
                break;
            default:
                row.setField(i, rs.getObject(i+1));
        }
    }
}
