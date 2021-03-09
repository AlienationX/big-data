package com.ego.spark;

import com.ego.HadoopUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

// clickhouse-integration-spark_2.11
// import org.apache.spark.sql.jdbc.JdbcDialects$;
// import org.apache.spark.sql.jdbc.ClickHouseDialect$;


public class DataSyncHiveToClickHouse {

    public static void main(String[] args) {
        SparkSession spark = HadoopUtil.createSparkSession("Hive To ClickHouse");

        // 必须注册
        // JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        // JdbcDialects.registerDialect(ClickHouseDriver);

        String fromTableName = args[0];
        String toTableName = args[1];
        String limitString = "";
        int partitionsNum = 10;
        if (args.length == 3) {
            limitString = " limit " + args[2];
        }

        Dataset<Row> df = spark.sql("select * from " + fromTableName + limitString);
        // df = df.repartition(partitionsNum);

        System.out.println("Total Rows: " + df.count());
        System.out.println("Total Partitions: " + df.toJavaRDD().partitions().size());
        df.printSchema();

        df.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")  // 官方jar包，会倒是jackson包冲突
                .option("url", "jdbc:clickhouse://10.63.80.111:8123/default")  // 官方8213端口，http性能不佳
                // .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")  // 第三方jar包
                // .option("url", "jdbc:clickhouse://10.63.80.111:9000")  // 第三方jar包使用9000端口，性能好像更快，但是未测试通。报错
                .option("user", "default")
                // .option("password", "password")
                .option("dbtable", toTableName)
                // .option("truncate", "true")
                // .option("batchsize", "10000")
                // .option("isolationLevel", "NONE") // 关闭事物
                .save();

        spark.stop();
    }
}
