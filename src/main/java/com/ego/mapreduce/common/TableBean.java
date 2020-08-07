package com.ego.mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 全用字符串来处理，txt格式本来也没有字段类型定义，在hive中定义成相应的数据格式即可。
 * <p>
 * ResultSet.getInt，ResultSet.getLong，ResultSet.getDouble等
 * 如果获取的数据库的值是null，默认会返回0。如果判断设置为null会报空指针的错误，处理起来非常麻烦！！！
 * <p>
 * ResultSet.getString会把null值替换成"null"字符串，如果想替换成hive的空值字符，需要转换成"\N"
 * 相当于sqoop下面参数的功能
 * --null-string '\\N' \
 * --null-non-string '\\N' \
 * <p>
 * Hive默认的分隔符是\001，属于不可见字符，这个字符在vi里是^A
 */

public class TableBean implements Writable, DBWritable {

    private static final String SEP = "\001";
    private static final String HIVE_NULL_CHAR = "\\N";
    // private static final String HIVE_NULL_CHAR = "null";

    private String values;

    @Override
    public String toString() {
        return this.values;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.values);
        // System.out.println("HDFS write: " + this.values);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.values = Text.readString(in);
        // System.out.println("HDFS read: " + this.values);
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        int columnCount = preparedStatement.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            preparedStatement.setObject(i, this.values.split(SEP)[i]);
        }
        // System.out.println("DB write: " + this.values);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        // 动态解析ResultSet
        List<String> rowData = new ArrayList<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String defaultValue = HIVE_NULL_CHAR;
            if (resultSet.getObject(i) != null) {
                defaultValue = resultSet.getObject(i).toString();
            }
            rowData.add(defaultValue);
        }
        this.values = String.join(SEP, rowData);
        // System.out.println("DB read: " + this.values);
    }

}
