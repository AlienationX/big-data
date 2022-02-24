package com.ego.spark;

import com.ego.HadoopUtil;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.LinkedList;
import java.util.List;

public class SparkKudu {

    public static void main(String[] args) {
        SparkSession spark = HadoopUtil.createSparkSession("Spark Kudu");
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")  // 一定要写，否则报错java.sql.SQLException: No suitable driver
                .option("url", "jdbc:mysql://hadoop-prod05:3306?useSSL=false")
                .option("user", "reader")
                .option("password", "reader@123!#")
                // .option("dbtable", "hive.COLUMNS_V2")
                // .option("dbtable", "(select t.*,now() as dt from hive.COLUMNS_V2 t) as T")
                // .option("dbtable", "(select uuid() as id, t.*, now() as dt from hive.COLUMNS_V2 t) as T")  -- 使用uuid解决主键问题
                .option("dbtable", "(select cast(t.cd_id as char) as cd_id,t.column_name,t.comment,t.type_name,t.integer_idx,now() as dt from hive.COLUMNS_V2 t) as T").load();
        df.printSchema();
        df.show();

        /*
        CREATE TABLE `COLUMNS_V2` (
          `CD_ID` string NOT NULL,
          `COLUMN_NAME` string NOT NULL,
          `COMMENT` string,
          `TYPE_NAME` string DEFAULT NULL,
          `INTEGER_IDX` int NOT NULL,
          `dt` timestamp,
          PRIMARY KEY (`CD_ID`, `COLUMN_NAME`)  -- 主键必须放在前面
        )
        PARTITION BY HASH PARTITIONS 16
        STORED AS KUDU;
         */

        // 注意事项：kudu没有库的概念。kudu的表分两种，kudu API自己创建的表，和外部表即impala创建的表。
        // 登录kudu web 页的tables标签，查看所有实际的表名(大小写敏感)。对于impala的表最底下可以看到CREATE EXTERNAL TABLE语句
        // 表名和导入导出的字段大小写需要一致，数据类型需要一致
        String kuduMaster = "hadoop-prod05:7051";
        String tableName = "impala::test_kudu.COLUMNS_V2";

        KuduContext kc = new KuduContext(kuduMaster, spark.sparkContext());
        if (!kc.tableExists(tableName)) {
            System.out.println(tableName + " does not exist.");
            // kc.createTable("impala::test_gp.student_info", kuduTableSchema, kuduTablePrimaryKey, kuduTableOptions);
            // List<String> key = new LinkedList<>();
            // index.add("cd_id");
            // index.add("column_name");
            // CreateTableOptions
            // kc.createTable("impala::test_gp.student_info", df.schema(), key,null);
        } else {
            System.out.println(tableName + " does exist.");
        }

        df.write()
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", kuduMaster)
                .option("kudu.table", tableName)
                .mode(SaveMode.Append)
                .save();

        spark.read()
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", kuduMaster)
                .option("kudu.table", tableName)
                .load()
                .show();
    }

}
