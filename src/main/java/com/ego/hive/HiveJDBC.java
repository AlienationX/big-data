package com.ego.hive;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveJDBC {

    // private Logger logger = Logger.getLogger(this.getClass());
    private static Logger logger = Logger.getLogger(HiveJDBC.class);

    // Oracle："oracle.jdbc.driver.OracleDriver" "jdbc:oracle:thin:@localhost:1521:db"
    // SqlServer2000："com.microsoft.jdbc.sqlserver.SQLServerDriver" "jdbc:microsoft:sqlserver://localhost:1433; DatabaseName=db"
    // SqlServer2005："com.microsoft.sqlserver.jdbc.SQLServerDriver" "jdbc:sqlserver://localhost:1433;DatabaseName=db"
    // MySql："com.mysql.jdbc.Driver" "jdbc:mysql://localhost:3306/db"

    // Hive
    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String URL = "jdbc:hive2://hadoop-prod05:10000/default";    // hive
    // private static String URL = "jdbc:hive2://hadoop-dev03:21050/;auth=noSasl";  // 可以直接使用impala
    private static String USER = "reader";
    private static String PWD = "reader123";


    public static void main(String[] args) {
        Connection conn;
        Statement stmt;
        ResultSet rs;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, USER, PWD);
            stmt = conn.createStatement();
            String sql = "show databases";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                // System.out.println(rs.getString("table_name"));  // mysql
                logger.info(rs.getString("database_name"));  // hive
                // logger.info(rs.getString("name"));  // impala
            }

            logger.info("-------------------------------------");
            String sqlStr = "select * from medical.dim_date where year=?";
            PreparedStatement pstmt = conn.prepareStatement(sqlStr);
            pstmt.setInt(1, 2019);
            ResultSet resultSet = pstmt.executeQuery();

            // 动态解析ResultSet
            List<String> tableData = new ArrayList<>();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                // logger.info(resultSet.getString("table_name"));
                Map<String, Object> rowData = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(resultSetMetaData.getColumnLabel(i), resultSet.getObject(i));
                }
                // tableData.add(rowData.toString());
                // tableData.add(JSON.toJSONString(rowData));
                tableData.add(JSON.toJSONStringWithDateFormat(rowData, "yyyy-MM-dd HH:mm:ss SSS"));
                logger.debug(JSON.toJSONString(rowData));
                logger.info(JSON.toJSONStringWithDateFormat(rowData, "yyyy-MM-dd HH:mm:ss SSS"));  // 支持时间格式的转换
            }
            logger.debug("-------------------------------------");
            System.out.println(tableData);

            // 批处理，拼接sql字符串插入数据
            // String sql_insert = "insert into stu values(?,?,?,?)";
            // pstmt = conm.prepareStatement(sql_insert);
            // for (int i = 0; i < 10; i++) {
            //     pstmt.setString(1, "S_10" + i);
            //     pstmt.setString(2, "stu" + i);
            //     pstmt.setInt(3, 20 + i);
            //     pstmt.setString(4, i % 2 == 0 ? "male" : "female");
            //     pstmt.addBatch();
            // }
            // executeBatch()返回受影响的行数
            // pstmt.executeBatch();

            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // } finally {
        //     if (rs != null) {
        //         try {
        //             rs.close();
        //         } catch (SQLException sqlEx) {
        //
        //         }
        //     }
        //
        //     if (stmt != null) {
        //         try {
        //             stmt.close();
        //         } catch (SQLException sqlEx) {
        //
        //         }
        //     }
        // }
    }
}
