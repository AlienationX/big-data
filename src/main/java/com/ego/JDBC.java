package com.ego;


import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.lang.Class;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBC {

    // private Logger logger = Logger.getLogger(this.getClass());
    private static Logger logger = Logger.getLogger(JDBC.class);

    // Oracle："oracle.jdbc.driver.OracleDriver" "jdbc:oracle:thin:@localhost:1521:db"
    // SqlServer2000："com.microsoft.jdbc.sqlserver.SQLServerDriver" "jdbc:microsoft:sqlserver://localhost:1433; DatabaseName=db"
    // SqlServer2005："com.microsoft.sqlserver.jdbc.SQLServerDriver" "jdbc:sqlserver://localhost:1433;DatabaseName=db"
    // MySql："com.mysql.jdbc.Driver" "jdbc:mysql://localhost:3306/db"

    private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static String URL = "jdbc:mysql://localhost:3306/etl";
    private static String USER = "root";
    private static String PWD = "root";

    public static void main(String[] args) {
        Connection conn;
        Statement stmt;
        ResultSet rs;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, USER, PWD);
            stmt = conn.createStatement();
            String sql = "select * from information_schema.tables where table_type='BASE TABLE'";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                // System.out.println(rs.getString("table_name"));
                logger.info(rs.getString("table_name"));
            }

            logger.info("-------------------------------------");
            String sqlStr = "select table_schema,table_name,engine,row_format,table_collation,table_rows,create_time from information_schema.tables where table_schema=? and table_type=?";
            PreparedStatement pstmt = conn.prepareStatement(sqlStr);
            pstmt.setString(1, "mysite");
            pstmt.setString(2, "BASE TABLE");
            ResultSet resultSet = pstmt.executeQuery();

            // 动态解析ResultSet
            List<String> tableData = new ArrayList<>();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                logger.info(resultSet.getString("table_name"));
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



