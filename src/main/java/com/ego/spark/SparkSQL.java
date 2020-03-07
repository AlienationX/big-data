package com.ego.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQL")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();

        // runHiveSQL(spark);
        runSaveCsv(spark);

        spark.stop();
    }

    private static void runHiveSQL(SparkSession spark) {
        Dataset<Row> sqlDF = spark.sql("select * from tmp.transactions");
        sqlDF.printSchema();
        sqlDF.show();
        System.out.println(sqlDF.count());

        // 临时表不能加库名，直接使用表名
        sqlDF.registerTempTable("transactions_tmp");

        // sqlDF.createTempView("transactions_tmp_v");  // 需要添加异常捕获AnalysisException
        sqlDF.createOrReplaceTempView("transactions_tmp_v_r");  // 当前会话显示，SparkSession.stop释放

        // sqlDF.createGlobalTempView("transactions_tmp_gv");  // 需要添加异常捕获AnalysisException
        sqlDF.createOrReplaceGlobalTempView("transactions_tmp_gv_r");  // 全局视图，当前会话不会显示，SparkContext.stop释放
        // Spark Application可以有多个Spark SparkSession

        // 表不存在会报错 Table or view not found: transactions_tmp;
        spark.sql("drop table if exists tmp.transactions_tmp_spark");
        spark.sql("create table tmp.transactions_tmp_spark stored as parquet as " +
                "select diseasename,count(*) as num from transactions_tmp group by diseasename");
        spark.sql("use tmp");
        spark.sql("show tables").show(100);
    }

    private static void runSaveCsv(SparkSession spark) {
        // save to csv
        Dataset<Row> sqlDFPart = spark.sql("select * from tmp.transactions limit 10");
        // spark.sql("drop table if exists tmp.transactions_limit");
        sqlDFPart.write().mode(SaveMode.Overwrite).saveAsTable("tmp.transactions_limit");  // spark默认存储格式为parquet
        sqlDFPart.limit(5).write()
                .option("header", "true")  // 是否写入header
                .option("delimiter", ",")   // 默认以”,”分割
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")    // 需要添加该选项，否则报错如下：Exception in thread "main" java.lang.IllegalArgumentException: Illegal pattern component: XXX
                .mode(SaveMode.Overwrite)
                .csv("file:///E:/Codes/Java/big-data/data/hive_data");  // 存储的为路径，程序会在路径下自动生成csv文件，和hdfs目录结构一致
        // .csv("file:///E:/Codes/Java/big-data/data/hive_data.csv");  // 不能指定文件名
        // .csv("hive_data.csv");  // 默认存储到hdfs路径
    }
}
