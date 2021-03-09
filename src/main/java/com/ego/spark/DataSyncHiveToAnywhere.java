package com.ego.spark;


import com.ego.HadoopUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class DataSyncHiveToAnywhere {

    public static void toHDFS(Dataset<Row> df) {

    }

    public static void toDB(Dataset<Row> df, String tableName) {
        // Saving data to a JDBC source
        df.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://hadoop-prod08:3306")
                .option("user", "todo")
                .option("password", "todolist")
                .option("dbtable", tableName)
                .save();
    }


    public static void toRedis(Dataset<Row> df) {

    }


    public static void toHBase(Dataset<Row> df) {

    }

    public static void toSolr(Dataset<Row> df) {

    }

    public static void toES(Dataset<Row> df, SparkSession spark) {
        //在spark中自动创建es中的索引
        spark.conf().set("es.index.auto.create", "true");
        //设置在spark中连接es的url和端口
        spark.conf().set("es.nodes", "master");
        spark.conf().set("es.port", "9200");

        JavaEsSparkSQL.saveToEs(df, "samples/table");
        // JavaEsSpark.saveToEs(rdd, "samples/table");
    }

    public static void main(String[] args) {
        SparkSession spark = HadoopUtil.createSparkSession("Hive To Anywhere");

        // Dataset<Row> df = spark.sql("select * from default.sample_07");
        Dataset<Row> df = spark.sql("select * from medical.std_dict");

        // toES(df, spark);
        toDB(df, "todo.std_dict");

        spark.stop();
    }
}
