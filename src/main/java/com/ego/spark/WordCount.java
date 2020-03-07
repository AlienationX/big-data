package com.ego.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        // SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        // SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("yarn-client");
        // JavaSparkContext spark = new JavaSparkContext(conf);

        // JavaRDD<String> lines = spark.textFile("hdfs:///user/work/tmp/input_wordcount");
        // JavaRDD<String> lines = spark.textFile("file:///E:/Codes/Java/big-data/data/word.txt");  // 省略默认读取的是hdfs上的文件

        // WARN SparkConf: spark.master yarn-client is deprecated in Spark 2.0+, please instead use "yarn" with specified deploy mode.
        // yarn模式需要上传jar才能远程提交执行
        // .config("spark.yarn.jars", "hdfs://namenode:8020/user/work/jars//lib/*.jar")  hdfs://hadoop000:8020/spark-yarn/jars/*.jar

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaWordCount")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();

        runHelloWorld(spark);

        spark.stop();
    }

    private static void runHelloWorld(SparkSession spark) {
        JavaRDD<String> lines = spark.read().textFile("file:///E:/Codes/Java/big-data/data/word.txt").javaRDD();
        // JavaRDD<String> lines = spark.read().textFile("hdfs:///user/work/tmp/input_wordcount").javaRDD();
        System.out.println(lines.collect());

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println(words.collect());

        // Dataset<Row> ds = spark.sql("show databases");
        // ds.show();
        // spark.sql("select * from medical.dim_date limit 100").show();
    }

}