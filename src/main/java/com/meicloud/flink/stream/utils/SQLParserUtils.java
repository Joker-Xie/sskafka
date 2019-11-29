package com.meicloud.flink.stream.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SQLParserUtils {

    public static String parserSQLAndGetTableNames(String sql, java.util.ArrayList<String> tableList) {
        String dbType = JdbcConstants.HIVE;
        List<SQLStatement> sqlList = SQLUtils.parseStatements(sql, dbType);
        List<String> allTabName = new ArrayList<String>();
        for (int i = 0; i < sqlList.size(); i++) {
            SQLStatement stmt = sqlList.get(i);
            MySqlSchemaStatVisitor version = new MySqlSchemaStatVisitor();
            stmt.accept(version);
            Iterator itor = version.getTables().keySet().iterator();
            while (itor.hasNext()) {
                allTabName.add(itor.next().toString());
            }
        }
        if (tableList != null) {
            allTabName.removeAll(tableList);
        }
        return allTabName.get(0);
    }
}
