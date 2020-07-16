package com.ego.spark;


import com.ego.HadoopUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class HiveDataSync {

    public static void readDB() {

    }

    public static void toDB(Dataset<Row> df) {

    }

    public static void readRedis() {

    }

    public static void toRedis(Dataset<Row> df) {


    }


    public static void readHBase() {

    }

    public static void toHBase(Dataset<Row> df) {

    }

    public static void readSolr() {

    }

    public static void toSolr(Dataset<Row> df) {

    }

    public static void readES() {

    }

    public static void toES(Dataset<Row> df, SparkSession spark) {
        //在spark中自动创建es中的索引
        spark.conf().set("es.index.auto.create", "true");
        //设置在spark中连接es的url和端口
        spark.conf().set("es.nodes", "master");
        spark.conf().set("es.port", "9200");

        JavaEsSparkSQL.saveToEs(df, "samples/table");
        // JavaEsSpark.saveToEs(jrdd, "samples/table");
    }


    public static void main(String[] args) {

        HadoopUtil.setEnvironment();

        SparkSession spark = HadoopUtil.createSparkSession("Hive Data Sync");

        Dataset<Row> df = spark.sql("select * from tmp.asd ");

        toES(df, spark);

        spark.stop();
    }
}
