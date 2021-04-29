package com.ego.spark;

import com.ego.HadoopUtil;
import com.ego.spark.DataSyncHiveSqlToClickHouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// clickhouse-integration-spark_2.11
// import org.apache.spark.sql.jdbc.JdbcDialects$;
// import org.apache.spark.sql.jdbc.ClickHouseDialect$;

public class DataSyncHiveTableToClickHouse {

    public static void generateCreateSql(SparkSession spark, String fromTableName, String toTableName, int isCreate) {

        // if toTableName not exists, then create.

        String query = "desc " + fromTableName;
        Dataset<Row> df = spark.sql(query);
        df.show(1000);
        df.printSchema();

        Map<String, String> map = new HashMap<>();
        map.put("boolean", "boolean");
        map.put("tinyint", "int");
        map.put("int", "int");
        map.put("integer", "int");
        map.put("bigint", "bigint");
        map.put("float", "float");
        map.put("double", "double");
        map.put("decimal", "decimal");
        map.put("string", "String");
        map.put("char", "String");
        map.put("varchar", "String");
        // map.put("binary", "binary");  // 存在问题
        map.put("timestamp", "timestamp");
        map.put("date", "date");
        // map.put("array", "String");
        // map.put("map", "String");
        // map.put("struct", "String");

        StringBuilder columnSql = new StringBuilder();
        String firstColumn = "";
        int i = 0;

        List<Row> result = df.toJavaRDD().collect();
        for (Row row : result) {
            String colName = row.getString(0);
            String dataType = row.getString(1);
            String comment = row.getString(2);

            // 分区表的字段没有作为ck的分区字段处理，只是当作普通字段，TODO
            if (colName.contains("#")) {
                System.out.println("WARNING: 分区表的字段没有作为ck的分区字段处理，只是当作普通字段，TODO");
                break;
            }

            for (String key : map.keySet()) {
                if (dataType.contains(key)) {
                    dataType = dataType.replaceAll(key, map.get(key));
                }
            }

            if (dataType.contains("array") || dataType.contains("map") || dataType.contains("struct") || dataType.contains("binary")) {
                System.out.println("存在复合数据类型 " + colName + " => " + dataType + "，请注意表结构");
                dataType = "String";
            }

            String dataTypeConvert = "Nullable(" + dataType + ")";
            i += 1;
            if (i == 1) {
                firstColumn = colName;
                dataTypeConvert = dataType;
            }

            columnSql.append(String.format("%-30s %-30s comment '%s',\n",
                    colName,
                    dataTypeConvert,
                    comment)
            );
        }

        String createSql = "CREATE TABLE IF NOT EXISTS " + toTableName + " (\n";

        createSql = createSql + columnSql.substring(0, columnSql.length() - 2) + "\n" +
                ")\n" +
                "ENGINE=MergeTree()\n" +
                "ORDER BY ("+ firstColumn + ");";

        System.out.println(createSql);

        if (isCreate != 0) {
            DataSyncHiveSqlToClickHouse.executeSql(createSql);
        }
    }

    public static void main(String[] args) {

        SparkSession spark = HadoopUtil.createSparkSession("HiveTable To ClickHouse");

        // JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        // JdbcDialects.registerDialect(ClickHouseDriver);

        String fromTableName = args[0];
        String toTableName = args[1];
        int partitionsNum = 0;
        int isCreate = 0;

        if (args.length == 3) {
            partitionsNum = Integer.parseInt(args[2]);
        }

        if (args.length == 4) {
            isCreate = Integer.parseInt(args[3]);
        }

        generateCreateSql(spark, fromTableName, toTableName, isCreate);

        String sqlString = "select * from " + fromTableName;
        DataSyncHiveSqlToClickHouse.dataSync(spark, sqlString, toTableName, partitionsNum);

        spark.stop();
    }
}
