package com.ego.algorthms.association.spark.examples;

import java.util.Arrays;
import java.util.List;

import com.ego.HadoopUtil;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;


public class FPGrowthExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("FP-Growth")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();

        List<Row> data = Arrays.asList(
                // RowFactory.create(Arrays.asList("1 2 5".split(" "))),
                // RowFactory.create(Arrays.asList("1 2 3 5".split(" "))),
                // RowFactory.create(Arrays.asList("1 2".split(" ")))

                RowFactory.create(Arrays.asList("A C D".split(" "))),
                RowFactory.create(Arrays.asList("B C E".split(" "))),
                RowFactory.create(Arrays.asList("A B C E".split(" "))),
                RowFactory.create(Arrays.asList("B E".split(" ")))

        );
        StructType schema = new StructType(new StructField[]{new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> itemsDF = spark.createDataFrame(data, schema);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(0.5)
                .setMinConfidence(0.6)
                .fit(itemsDF);

        // Display frequent itemsets.
        model.freqItemsets().show();

        // Display generated association rules.
        model.associationRules().show();

        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        model.transform(itemsDF).show();



        // Dataset<Row> df = model.freqItemsets();
        // df.sort(col("freq").desc()).show();
        // df.printSchema();
        // df.select(col("items").cast(ArrayType));
        // df = df.withColumn("k", df.col("items").cast(ArrayType));
        // df.printSchema();
        // df.show();

        // 去除包含项
        // 方法1. collect获取结果遍历，仅支持小数据量
        // model.freqItemsets().toJavaRDD().collect();

        // 方法2. 转换成RDD，广播结果集，map + filter，比较麻烦但是推荐
    }
}
