package com.ego.spark;

import com.ego.HadoopUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.apache.spark.ui.jobs.JobsTab;

import java.util.Arrays;
import java.util.Objects;

/**
 * executor-memory 6g       # yarn查看1个core对应4G
 * executor-memory 8g       # yarn查看1个core对应5G
 * executor-memory 10g      # yarn查看1个core对应6G
 *
 * spark2-submit \
 * --master yarn \
 * --deploy-mode client \
 * --driver-memory 1g \
 * --executor-memory 6g \
 * --queue root.flow \
 * --jars mysql-connector-java-5.1.48.jar \
 * --class com.ego.spark.DataSyncAnywhereToHive \
 * bigdata-1.0-SNAPSHOT.jar
 */
public class DataSyncAnywhereToHive {

    public static Dataset<Row> readHDFS(SparkSession spark) {
        String path = "/user/work/tmp/input_wordcount/wordcount.txt";

        Dataset<String> strDF = spark.read().textFile(path);
        strDF.show();

        JavaRDD<String> words = spark.read().textFile(path).toJavaRDD()
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(s -> s != null && !"".equals(s));
        System.out.println(words.collect());

        Dataset<Row> df = spark.read().text(path);
        df.show();
        return df;
    }

    public static Dataset<Row> readDB(SparkSession spark) {
        // Dataset<Row> jdbcDF = spark.read()
        //         .format("jdbc")
        //         .option("url", "jdbc:postgresql:dbserver")
        //         .option("dbtable", "schema.tablename")
        //         .option("user", "username")
        //         .option("password", "password")
        //         .load();
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://hadoop-prod08:3306/todo?charset=utf-8&useSSL=false&tinyInt1isBit=false")
                .option("user", "todo")
                .option("password", "todolist")
                .option("dbtable", "todo.ods_xnh_personal_info")
                .load();
        return jdbcDF;
    }

    public static Dataset<Row> readRedis() {
        Dataset<Row> df = null;
        return df;
    }

    public static Dataset<Row> readHBase() {
        Dataset<Row> df = null;
        return df;
    }


    public static Dataset<Row> readKudu() {
        Dataset<Row> df = null;
        return df;
    }

    public static Dataset<Row> readSolr() {
        Dataset<Row> df = null;
        return df;
    }


    public static Dataset<Row> readES(SparkSession spark) {
        //在spark中自动创建es中的索引
        spark.conf().set("es.index.auto.create", "true");
        //设置在spark中连接es的url和端口
        spark.conf().set("es.nodes", "master");
        spark.conf().set("es.port", "9200");

        Dataset<Row> df = null;
        return df;
    }

    public static void toHive(Dataset<Row> df, String tableName, SparkSession spark) {
        // df.write().mode(SaveMode.Overwrite).saveAsTable(tableName);

        df.createOrReplaceTempView("tmp_table");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " stored as parquet as select * from tmp_table");
    }

    public static void main(String[] args) {
        SparkSession spark = HadoopUtil.createSparkSession("Anywhere To Hive");

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://hadoop-prod08:3306/todo?charset=utf-8&useSSL=false&tinyInt1isBit=false")
                .option("user", "todo")
                .option("password", "todolist")
                .option("dbtable", "(select * from todo.users limit 2) as t")
                .load();
        jdbcDF.show();

        readHDFS(spark);

        // Dataset<Row> df = readDB(spark);
        // toHive(df, "tmp.tmp_spark_table", spark);

        spark.stop();
    }
}
