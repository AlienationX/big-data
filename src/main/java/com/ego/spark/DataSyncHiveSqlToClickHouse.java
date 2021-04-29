package com.ego.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;

// clickhouse-integration-spark_2.11
// import org.apache.spark.sql.jdbc.JdbcDialects$;
// import org.apache.spark.sql.jdbc.ClickHouseDialect$;

import com.ego.HadoopUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DataSyncHiveSqlToClickHouse {

    private static final String CK_DRIVER = "com.github.housepower.jdbc.ClickHouseDriver";
    private static final String CK_URL = "jdbc:clickhouse://10.63.82.203:9001";
    private static final String CK_USERNAME = "ck";
    private static final String CK_PASSWORD = "ck";

    public static void generateCreateSql(SparkSession spark, String sqlString, String toTableName, int isCreate) {
        /**
         * ------------------------------------------------------------------------
         * drop table if exists tmp.struct_field;
         * create table if not exists tmp.struct_field (
         * -- 原始类型
         * c1  BOOLEAN                            ,  --  true/false    TRUE
         * c2  TINYINT                            ,  --  1字节的有符号整数 -128~127    1Y
         * c3  SMALLINT                           ,  --  2个字节的有符号整数，-32768~32767    1S
         * c4  INT                                ,  --  4个字节的带符号整数    1
         * c5  BIGINT                             ,  --  8字节带符号整数    1L
         * c6  FLOAT                              ,  --  4字节单精度浮点数1.0
         * c7  DOUBLE                             ,  --  8字节双精度浮点数    1.0
         * c8  DECIMAL(38,4)                      ,  --  任意精度的带符号小数，不指定精度默认(10,0)    1.0
         * c9  STRING                             ,  --  字符串，变长    "a",'b'
         * c10 VARCHAR(256)                       ,  --  变长字符串    "a",'b'
         * c11 CHAR(20)                           ,  --  固定长度字符串    "a",'b'
         * c12 BINARY                             ,  --  字节数组    无法表示
         * c13 TIMESTAMP                          ,  --  时间戳，纳秒精度    122327493795
         * c14 DATE                               ,  --  日期    '2016-03-29'
         *
         * -- 复杂类型
         * c15 ARRAY<INT>                         ,  --  有序的的同类型的集合    array(1,2)
         * c16 MAP<STRING,INT>                    ,  --  key-value,key必须为原始类型，value可以任意类型    map('a',1,'b',2)
         * c17 STRUCT<a:STRING,b:INT,c:DOUBLE>       --  字段集合,类型可以不同    struct('1',1,1.0), named_stract('col1','1','col2',1,'clo3',1.0)
         * -- c18 UNIONTYPE<STRING,INT>              --  在有限取值范围内的一个值，sparksql无法识别    create_union(1,'a',63)
         * );
         *
         * ClickHouse的字段数据类型参考，映射到别名即可。case_insensitive为1的不区分大小写。
         * SELECT * FROM system.data_type_families WHERE alias_to = 'String'
         *
         * ClickHouse的Array和Map类型不能使用Nullable，且Map类型必须开启set allow_experimental_map_type = 1;
         * DROP TABLE IF EXISTS test.std_struct_field;
         * CREATE TABLE IF NOT EXISTS test.std_struct_field (
         * c1                             Nullable(Boolean),
         * c2                             int,
         * c3                             Nullable(int),  -- 不支持short
         * c4                             Nullable(int),
         * c5                             Nullable(bigint),  -- 不支持long
         * c6                             Nullable(Float),
         * c7                             Nullable(Double),
         * c8                             Nullable(Decimal(38,4)),
         * c9                             Nullable(String),
         * c10                            Nullable(String),
         * c11                            Nullable(String),
         * -- c12                            Nullable(Binary),  -- 存在问题
         * c13                            Nullable(Timestamp),
         * c14                            Nullable(Date),
         * c15                            Array(Integer),  -- sparkSql可以读取不能写
         * c16                            Map(String,Integer),  --
         * -- c17                            STRUCT(String)  -- 不支持
         * -- c18                            UNION(String),  -- sparkSql无法识别
         * c19                            Nullable(String)   -- 占位测试字段
         * )
         * ENGINE=MergeTree()
         * ORDER BY (c2);
         */

        // if toTableName not exists, then create.

        // String sql = "select * from (" + sqlString + ") t where 1=2";
        String query = "select * from (" + sqlString + ") limit 1";
        Dataset<Row> df = spark.sql(query);
        df.printSchema();

        Map<String, String> map = new HashMap<>();
        map.put("BooleanType", "boolean");
        map.put("ByteType", "int");  // TINYINT转成了Byte？
        map.put("ShortType", "int");
        map.put("IntegerType", "int");
        map.put("LongType", "bigint");
        map.put("FloatType", "float");
        map.put("DoubleType", "double");
        map.put("DecimalType", "decimal");
        map.put("StringType", "String");  // 或者映射text？
        // map.put("StringType", "");
        // map.put("StringType", "");
        // map.put("BinaryType", "binary");  // 存在问题
        map.put("TimestampType", "timestamp");
        map.put("DateType", "date");
        map.put("ArrayType", "Array");
        map.put("MapType", "Map");
        // map.put("StructType", "text");  // ck不存在该数据类型

        StructType schema = df.schema();
        StringBuilder columnSql = new StringBuilder();
        String firstColumn = "";
        int i = 0;
        for (StructField field : schema.fields()) {
            // System.out.println(field.name() + " = " + field.dataType().toString() + " ," + field.dataType().typeName());

            String dataType = field.dataType().toString();
            for (String key : map.keySet()) {
                if (dataType.contains(key)) {
                    dataType = dataType.replaceAll(key, map.get(key));
                }
            }

            if (dataType.contains("Array") || dataType.contains("Map") || dataType.contains("Struct") || dataType.contains("Binary")) {
                System.out.println("存在复合数据类型 " + field.name() + " => " + dataType + "，请注意表结构");
                dataType = "String";
            }

            String dataTypeConvert = "Nullable(" + dataType + ")";
            i += 1;
            if (i == 1) {
                firstColumn = field.name();
                dataTypeConvert = dataType;
            }

            columnSql.append(String.format("%-30s %s,\n",
                    field.name(),
                    dataTypeConvert)
            );
        }
        String createSql = "CREATE TABLE IF NOT EXISTS " + toTableName + " (\n";

        createSql = createSql + columnSql.substring(0, columnSql.length() - 2) + "\n" +
                ")\n" +
                "ENGINE=MergeTree()\n" +
                "ORDER BY (" + firstColumn + ");";

        System.out.println(createSql);

        if (isCreate != 0) {
            executeSql(createSql);
        }
    }

    public static void executeSql(String createSql) {
        try {
            Class.forName(CK_DRIVER);
            Connection conn = DriverManager.getConnection(CK_URL, CK_USERNAME, CK_PASSWORD);
            Statement stmt = conn.createStatement();
            // ResultSet rs = stmt.executeQuery(createSql);
            // int result = stmt.executeUpdate(createSql);
            stmt.executeUpdate(createSql);
            stmt.close();
            conn.close();
            System.out.println("===> ClickHouse table is created.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dataSync(SparkSession spark, String sqlString, String toTableName, int partitionsNum) {
        LongAccumulator accumulatorRowNum = spark.sparkContext().longAccumulator("accumulatorNum");

        Dataset<Row> df = spark.sql(sqlString);
        if (partitionsNum != 0) {
            df.repartition(partitionsNum);
        }

        df.cache();
        // df.foreach((ForeachFunction<Row>) x->accumulatorRowNum.add(1));
        long rowNum = df.count();

        df.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                // .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")  // 官方jar包，会倒是jackson包冲突
                // .option("url", "jdbc:clickhouse://10.63.80.111:8123/default")  // 官方8213端口，http性能不佳
                .option("driver", CK_DRIVER)  // 第三方jar包
                .option("url", CK_URL)  // 第三方jar包使用9000端口，性能好像更快，但是未测试通。报错
                .option("user", CK_USERNAME)
                .option("password", CK_PASSWORD)
                .option("dbtable", toTableName)
                // .option("truncate", "true")
                // .option("isolationLevel", "NONE") // 关闭事物
                // .option("numPartitions", "1") // 设置并发，否则会报merges are processing significantly slower than inserts，但是会变得很慢
                .option("batchsize", "1000000")  // clickhouse推荐批量插入，官方建议“每秒不超过1次的insert request”，太频繁的提交会报merges are processing significantly slower than inserts，所以推荐加大该参数
                .save();

        df.printSchema();
        System.out.println("Total Partitions: " + df.toJavaRDD().partitions().size());
        System.out.println("Total Rows: " + rowNum);
        // System.out.println("Total Rows LongAccumulator: " + accumulatorRowNum.value());

        // spark.sql("select * from " + fromTableName + limitString)
        //         .repartition(partitionsNum)
        //         .write()
        //         .mode(SaveMode.Append)
        //         .format("jdbc")
        //         // .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")  // 官方jar包，会倒是jackson包冲突
        //         // .option("url", "jdbc:clickhouse://10.63.80.111:8123/default")  // 官方8213端口，http性能不佳
        //         .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")  // 第三方jar包
        //         .option("url", "jdbc:clickhouse://10.63.82.203:9001")  // 第三方jar包使用9000端口，性能好像更快，但是未测试通。报错
        //         .option("user", "ck")
        //         .option("password", "ck")
        //         .option("dbtable", toTableName)
        //         // .option("truncate", "true")
        //         // .option("batchsize", "10000")
        //         // .option("isolationLevel", "NONE") // 关闭事物
        //         .save();

    }

    public static void main(String[] args) {

        SparkSession spark = HadoopUtil.createSparkSession("HiveSql To ClickHouse");

        // JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        // JdbcDialects.registerDialect(ClickHouseDriver);

        String sqlString = args[0];
        String toTableName = args[1];
        int partitionsNum = 0;
        int isCreate = 0;

        if (args.length == 3) {
            partitionsNum = Integer.parseInt(args[2]);
        }

        if (args.length == 4) {
            isCreate = Integer.parseInt(args[3]);
        }

        generateCreateSql(spark, sqlString, toTableName, isCreate);

        dataSync(spark, sqlString, toTableName, partitionsNum);

        spark.stop();
    }
}
