package com.ego.examples;


import com.alibaba.fastjson.JSON;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook; // xls
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook; // xlsx

import java.awt.Color;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.lang.Class;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CaseJDBC {

    // private Logger logger = Logger.getLogger(this.getClass());
    private static Logger LOG = Logger.getLogger(CaseJDBC.class);

    // Oracle："oracle.jdbc.driver.OracleDriver" "jdbc:oracle:thin:@localhost:1521:db"
    // SqlServer2000："com.microsoft.jdbc.sqlserver.SQLServerDriver" "jdbc:microsoft:sqlserver://localhost:1433; DatabaseName=db"
    // SqlServer2005："com.microsoft.sqlserver.jdbc.SQLServerDriver" "jdbc:sqlserver://localhost:1433;DatabaseName=db"
    // MySql："com.mysql.jdbc.Driver" "jdbc:mysql://localhost:3306/db"

    // MySql
    private static String JDBC_DRIVER = null;
    private static String URL = null;
    private static String USER = null;
    private static String PWD = null;

    // public CaseJDBC() throws IOException {
    public static void init() throws IOException {
        InputStream in = new FileInputStream("conf/db.properties");
        Properties prop = new Properties();
        prop.load(in);
        JDBC_DRIVER = prop.getProperty("jdbc.mysql.local.driver");
        URL = prop.getProperty("jdbc.mysql.local.url");
        USER = prop.getProperty("jdbc.mysql.local.username");
        PWD = prop.getProperty("jdbc.mysql.local.password");

        LOG.setLevel(Level.DEBUG);
    }

    public static void main(String[] args) {
        Connection conn;
        Statement stmt;
        ResultSet rs;
        try {
            init();
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, USER, PWD);
            stmt = conn.createStatement();
            String sql = "select * from information_schema.tables where table_type='BASE TABLE'";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                // System.out.println(rs.getString("table_name"));
                LOG.info(rs.getString("table_schema") + "." + rs.getString("table_name"));
            }

            LOG.info("-------------------------------------");
            String sqlStr = "select table_schema,table_name,engine,row_format,table_collation,table_rows,create_time from information_schema.tables where table_schema=? and table_type=?";
            PreparedStatement pstmt = conn.prepareStatement(sqlStr);
            System.out.println(pstmt.getParameterMetaData());
            System.out.println(pstmt.getMetaData());
            System.out.println(pstmt.getClass());
            pstmt.setString(1, "test");
            pstmt.setString(2, "BASE TABLE");
            ResultSet resultSet = pstmt.executeQuery();

            // 动态解析ResultSet
            List<String> tableData = new ArrayList<>();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                LOG.info(resultSet.getString("table_name"));
                Map<String, Object> rowData = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(resultSetMetaData.getColumnLabel(i), resultSet.getObject(i));
                }
                // tableData.add(rowData.toString());
                // tableData.add(JSON.toJSONString(rowData));
                tableData.add(JSON.toJSONStringWithDateFormat(rowData, "yyyy-MM-dd HH:mm:ss SSS"));
                LOG.debug(JSON.toJSONString(rowData));
                LOG.info(JSON.toJSONStringWithDateFormat(rowData, "yyyy-MM-dd HH:mm:ss SSS"));  // 支持时间格式的转换
            }
            LOG.debug("-------------------------------------");
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


            ResultSet rsExcel = stmt.executeQuery(sql);
            // ResultSet rsExcel = pstmt.executeQuery();
            toExcel(rsExcel, "data/jdbcToExcel.xlsx");

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

    private static void toExcel(ResultSet rs, String outName) {
        try {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            XSSFWorkbook wb = new XSSFWorkbook();
            XSSFSheet sheet = wb.createSheet("sheet");
            sheet.createFreezePane(0, 1); // 首行冻结
            // sheet.createFreezePane(1, 3); // 冻结A列3行

            // 写入表头字段
            XSSFFont headerFont = wb.createFont();
            headerFont.setFontName("Arial");  // xlsx默认字体Calibri
            headerFont.setFontHeightInPoints((short) 10);  // 字体大小
            headerFont.setBold(true);  // 加粗
            // header的样式
            XSSFCellStyle headerStyle = wb.createCellStyle();
            headerStyle.setFont(headerFont);
            headerStyle.setAlignment(HorizontalAlignment.LEFT); // 左右居中
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND); // 设置填充方案
            headerStyle.setFillForegroundColor((short) 13); // 设置填充背景色，13为黄色
            headerStyle.setBorderLeft(BorderStyle.THIN);  // 左边框
            headerStyle.setBorderRight(BorderStyle.THIN);  // 右边框
            // headerStyle.setLocked(true);  // ???
            // headerStyle.setWrapText(true); // 自动换行
            XSSFRow rowHeader = sheet.createRow(0);
            for (int j = 1; j <= columnCount; j++) {
                String colName = rs.getMetaData().getColumnLabel(j);
                XSSFCell cell = rowHeader.createCell(j - 1);
                cell.setCellStyle(headerStyle);
                cell.setCellValue(colName);
            }

            // 写入数据
            XSSFFont dataFont = wb.createFont();
            dataFont.setFontName("Arial");  // xlsx默认字体Calibri
            dataFont.setFontHeightInPoints((short) 10);  // 字体大小
            // data的样式
            XSSFCellStyle dataStyle = wb.createCellStyle();
            dataStyle.setFont(dataFont);
            int i = 1;
            while (rs.next()) {
                System.out.println(i);
                XSSFRow rowData = sheet.createRow(i);
                for (int j = 1; j <= columnCount; j++) {
                    // 数字也都处理成字符串了，更严格的话需要进行类型判断
                    String val = rs.getString(j);
                    XSSFCell cell = rowData.createCell(j - 1);
                    cell.setCellStyle(dataStyle);
                    cell.setCellValue(val);
                }
                i++;
            }
            FileOutputStream foStream = new FileOutputStream(outName);
            wb.write(foStream);
            foStream.flush();
            foStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



