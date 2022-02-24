package com.ego.datalake;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * 版本冲突，推荐使用maven的多模块管理
 */

public class Example {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("hudi example")
                .config("spark.some.config.option", "some-value")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 使用hudi必须设置
                .config("spark.sql.hive.convertMetastoreParquet", "false") // Uses Hive SerDe, this is mandatory for MoR tables
                .enableHiveSupport().getOrCreate();

        spark.sql("show databases").show();

        Dataset<Row> data = spark.sql("" +
                "select 1 as id, 'aaa' as name, 55 as price, '2020-01-01' as update_date union all " +
                "select 2 as id, 'bbb' as name, 55 as price, '2020-01-01' as update_date union all " +
                "select 3 as id, 'ccc' as name, 55 as price, '2020-01-01' as update_date"
        );
        data.show();
        String basePath = "/user/work/tmp/tables/";
        String tableName = "h1";
        data.write()
                .format("org.apache.hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "update_date")  // 设置???
                .option(HoodieWriteConfig.TABLE_NAME, tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 并行
                .mode(SaveMode.Overwrite)
                .save(basePath + tableName);

        // spark.read().format("hudi").load(basePath + tableName).show();

        String sql1 = "-- create a managed cow table\n" + "create table if not exists h1 (\n" + "  id int, \n" + "  name string, \n" + "  price double\n" + ") using hudi\n" + "options (\n" + "  type = 'cow',\n" + "  primaryKey = 'id'\n" + ");";
        String sql2 = "create table h2 using hudi\n" + "options (type = 'cow', primaryKey = 'id')\n" + "partitioned by (dt)\n" + "as\n" + "select 1 as id, 'a1' as name, 10 as price, 1000 as dt;";
        String sql3 = "create table h3 using hudi as select 1 as id, 'a1' as name, 10 as price";

        // spark.sql(sql3);
        spark.sql("show tables").show();

        spark.stop();
    }
}
