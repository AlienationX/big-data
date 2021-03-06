package com.ego.algorthms.association.spark;


import com.ego.algorthms.association.Combination;
import com.ego.HadoopUtil;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;


public class FindAssociationRules {

    private final static int MAX_COMBINATION_NUM = 10;

    public static Dataset<Row> getDataset(SparkSession spark) {
        // Dataset<Row> sqlDF = spark.sql("select * from tmp.transactions");
        //
        // sqlDF.printSchema();
        // // sqlDF.show(10);
        // // sqlDF.select("items").show(10);
        // // sqlDF.select(col("visitid"), col("items")).show(5);
        // return sqlDF.select("items");

        return spark.sql("select clientids as items from tmp.transactions_zy");
    }

    public static JavaRDD<Row> createLk(SparkSession spark, JavaRDD<Row> transactions, long rowNum, double minSupport, int k) {
        // 内置的累加器有三种，LongAccumulator、DoubleAccumulator、CollectionAccumulator
        // LongAccumulator: 数值型累加
        // LongAccumulator combinationNum = spark.sparkContext().longAccumulator("combinationNum");

        // map
        JavaPairRDD<List<String>, Integer> combinationGroupOne = transactions.flatMapToPair(new PairFlatMapFunction<Row, List<String>, Integer>() {
            @Override
            public Iterator<Tuple2<List<String>, Integer>> call(Row row) throws Exception {
                List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
                String transaction = row.getAs("items");

                List<String> itemsList = new ArrayList<>(Arrays.asList(transaction.split(",")));
                if (k == 1) {
                    for (String item : itemsList) {
                        List<String> combination = new ArrayList<>(Collections.singletonList(item));
                        result.add(new Tuple2<>(combination, 1));
                    }
                } else {
                    List<List<String>> combinations = Combination.findSortedCombinations(itemsList, k);
                    for (List<String> combination : combinations) {
                        if (combination.size() > 0) {
                            result.add(new Tuple2<>(combination, 1));
                        }
                    }
                }

                return result.iterator();
            }
        });

        // reduce frequent set
        JavaPairRDD<List<String>, Integer> ck = combinationGroupOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<List<String>, Integer> frequentSet = ck.filter(s -> s._2.doubleValue() / rowNum > minSupport);
        int saveRowNum = (int) frequentSet.count();
        System.out.println("-- Output --" + k);
        System.out.println("Total combination save " + saveRowNum + " rows.");

        // for (Tuple2<List<String>, Integer> tuple : frequentSet.collect()) {
        //     System.out.println(tuple._1 + ": " + tuple._2);
        // }

        // frequentSet.foreach(new VoidFunction<Tuple2<List<String>, Integer>>() {
        //     @Override
        //     public void call(Tuple2<List<String>, Integer> listIntegerTuple2) throws Exception {
        //         System.out.println(listIntegerTuple2);
        //     }
        // });

        // JavaPair convert JavaRDD<Row>
        JavaRDD<Row> rowRDD = frequentSet.map(new Function<Tuple2<List<String>, Integer>, Row>() {
            @Override
            public Row call(Tuple2<List<String>, Integer> v1) throws Exception {
                String a = String.join(",", v1._1);
                Integer b = v1._2;
                return RowFactory.create(a, b, rowNum);
            }
        });

        return rowRDD;
    }

    public static void runFindFrequentSet(SparkSession spark, double minSupport) {
        Dataset<Row> df = getDataset(spark);
        // 持久化缓存 sqlDF.persist();
        df.cache();
        long rowNum = df.count();
        System.out.println("Total Rows: " + rowNum);
        df.show(10);

        // JavaRDD<String> transactions = df.toJavaRDD().map(s -> s.get(0));
        // JavaRDD<String> transactions = df.toJavaRDD().map(s -> s.getString(0));
        // JavaRDD<String> transactions = df.toJavaRDD().map(s -> s.getAs("items"));
        JavaRDD<Row> transactions = df.toJavaRDD();
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());
        transactions = transactions.repartition(32);
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());

        // result DataFrame
        ArrayList<StructField> fields = new ArrayList<>();
        Map<String, DataType> map = new HashMap<String, DataType>() {{
            put("frequent_set", DataTypes.StringType);
            put("num", DataTypes.IntegerType);
            put("row_num", DataTypes.LongType);
        }};
        for (String columnName : map.keySet()) {
            fields.add(DataTypes.createStructField(columnName, map.get(columnName), true));
        }
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> frequentSetDF = spark.createDataFrame(new ArrayList<>(), schema);

        for (int i = 1; i <= MAX_COMBINATION_NUM; i++) {
            JavaRDD<Row> frequentSetRDD = createLk(spark, transactions, rowNum, minSupport, i);
            if (frequentSetRDD.isEmpty()) {
                System.out.println("L" + i + " no data, abort.");
                break;
            }
            Dataset<Row> frequentSetDS = spark.createDataFrame(frequentSetRDD, schema);
            frequentSetDF = frequentSetDF.unionAll(frequentSetDS);
        }

        // save, 有时候会报权限不足的错误或警告
        // frequentSet.saveAsTextFile("/user/work/tmp/frequent_set");
        frequentSetDF.write().mode(SaveMode.Overwrite).saveAsTable("tmp.frequent_set");  // 如果表不存在，会默认创建，默认格式为parquet
        // result1.write().partitionBy("dt").format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.frequent_set");  // 如果表不存在，会默认创建，默认格式为parquet

        // frequentSetDF.registerTempTable("frequent_set");  // 已过时
        frequentSetDF.createOrReplaceTempView("frequent_set");
        spark.sql("drop table if exists tmp.frequent_set_tmp");
        spark.sql("create table tmp.frequent_set_tmp stored as parquet as select t.*,t.num/t.row_num as support from frequent_set t");  // 默认textfile，需要额外指定存储格式
    }

    public static void runFindAssociationRules(SparkSession spark, double minConfidence) {
        // TODO
    }

    public static void main(String[] args) {

        // STEP-1: handle input parameters
        if (args.length < 2) {
            System.err.println("Usage: FindAssociationRules <minSupport> <minConfidence>");
            System.exit(1);
        }
        double minSupport = Double.parseDouble(args[0]);
        double minConfidence = Double.parseDouble(args[1]);
        // String tableName = args[2];
        // String colName = args[3];

        // STEP-2: create a SparkSession
        SparkSession spark = HadoopUtil.createSparkSession("Find Association Rules");

        runFindFrequentSet(spark, minSupport);
        runFindAssociationRules(spark, minConfidence);

        spark.stop();
    }

}
