import com.alibaba.druid.pool.DruidDataSource;

import java.sql.*;
import java.util.Properties;

public class MysqlOps {
    private static DruidDataSource source = new DruidDataSource();

    static {
        source.setDriverClassName("com.mysql.jdbc.Driver");
        //建立连接
        source.setUrl("jdbc:mysql://localhost:3306/mydb?serverTimezone=Asia/Shanghai");
        source.setUsername("root");
        source.setPassword("123456");
    }

    public String[] getDBMeta() {
        try {
            Connection conn = source.getConnection();
            String sql = "select * from t_user_info limit 0 ";
            PreparedStatement pst = conn.prepareStatement(sql);
            ResultSetMetaData metaData = pst.getMetaData();
            int metaClum = metaData.getColumnCount();
            for (int i = 1; i <= metaClum; i++) {
                System.out.println(metaData.getColumnName(i)+":"+metaData.getColumnClassName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args) {
        MysqlOps mysqlOps = new MysqlOps();
        String[] test = mysqlOps.getDBMeta();
    }
}
