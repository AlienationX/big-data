package com.ego.spark.datamining;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Apriori {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Apriori")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> sqlDF = spark.sql("select * from tmp.transactions");
        sqlDF.printSchema();
        sqlDF.show();
        System.out.println(sqlDF.count());



        spark.stop();
    }
}
