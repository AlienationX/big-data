package com.ego.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSync {

    // public static Dataset<Row> getJDBC(SparkSession spark){
    //
    // }
    //
    // public static Dataset<Row> getES(SparkSession spark){
    //
    // }
    //
    // public static Dataset<Row> getSorl(SparkSession spark){
    //
    // }
    //
    // public static Dataset<Row> getHBase(SparkSession spark){
    //
    // }
    //
    // public static Dataset<Row> getRedis(SparkSession spark){
    //
    // }
    //
    // public static void toHive(SparkSession spark){
    //
    // }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Data Sync")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .enableHiveSupport()
                .getOrCreate();


        spark.stop();
    }
}
