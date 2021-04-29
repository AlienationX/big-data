package com.ego.algorthms.association.spark;

import com.ego.HadoopUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 基于每行数据的item进行组合
 * 两两组合公式：N*(N-1)/2
 *
 * 只计算真实存在的组合，会有重复最终ck需要去重，但是两两组合的数量确实会少些
 * -- 如果一行的最大两目数是2000，那二项集项目就有1999000的组合
 * -- 2000*1999/2 = 1999000
 *
 * 3个关键的数据变量
 * JavaRDD<Row>                       transactions         原始数据集
 * JavaRDD<List<String>               ck                   k项频繁项集
 * JavaPairRDD<Integer, List<String>  transactionsPairRDD  基于ck组合的数据集
 */

public class AprioriWithRowItemCombinationOptimize {

    private final static Logger logger = Logger.getLogger("Apriori");
    // private final static Logger logger = Logger.getLogger(AprioriWithRowItemCombination.class);

    private final static int MAX_COMBINATION_NUM = 10;
    private final static String DEFAULT_PARTITIONS = "32";
    private final static String STD_COLUMN = "items";

    public static List<String> getWhiteList() {
        List<String> whiteList = new ArrayList<>();
        whiteList.add("");
        whiteList.add("null");
        whiteList.add("NULL");
        return whiteList;
    }

    public static JavaRDD<Row> transactionsConvert(JavaRDD<Row> transactions, JavaRDD<List<String>> ck) {
        List<List<String>> ckUniqueList = ck.collect();

        return transactions.map(row -> {
            List<List<String>> result = new ArrayList<>();

            List<String> itemList = row.getAs(0);
            for (List<String> oneItems : ckUniqueList) {
                if (itemList.containsAll(oneItems)) {
                    result.add(oneItems);
                }
            }
            return RowFactory.create(result);
        }).filter(row -> ((List) row.getAs(0)).size() > 0);
    }

    public static JavaRDD<Row> transactionsFilter(JavaRDD<Row> transactionsRowCk, JavaRDD<List<String>> ck) {
        List<List<String>> ckUniqueList = ck.collect();

        return transactionsRowCk.map(row -> {
            List<List<String>> itemList = row.getAs(0);
            itemList.retainAll(ckUniqueList);
            return RowFactory.create(itemList);
        }).filter(row -> ((List) row.getAs(0)).size() > 0);
    }

    public static void showTransactionsOverview(SparkSession spark, JavaRDD<Row> transactionsRowCk, int k) {
        // 内置的累加器有三种，LongAccumulator、DoubleAccumulator、CollectionAccumulator
        // LongAccumulator: 数值型累加
        LongAccumulator accumulatorNum = spark.sparkContext().longAccumulator("accumulatorNum");
        accumulatorNum.setValue(0);

        System.out.println("Output>>> Show " + k + " Transactions Top 5");
        // transactionsRowCk.map(row -> {
        //     accumulatorNum.add(1);
        //     return ((List) row.getAs(0)).size();
        // })
        //         .top(5)
        //         .forEach(System.out::println);

        // JavaPairRDD<Integer, String> data = transactionsRowCk.mapToPair(row -> {
        //     accumulatorNum.add(1);
        //     // return new Tuple2<>(((List) row.getAs(0)).size(), row.getAs(0).toString());
        //     return new Tuple2<>(((List) row.getAs(0)).size(), "");
        // });
        //
        // data.cache();
        //
        // System.out.println("Output>>> Show " + k + " Transactions Top 5");
        // data.sortByKey(false)
        //         .map(x -> x._1)
        //         .take(5)
        //         .forEach(System.out::println);

        // System.out.println("Output>>> Show " + k + " Transactions Count " + data.count());
        System.out.println("Output>>> Show " + k + " Transactions LongAccumulator " + accumulatorNum.value());
    }

    public static void saveCkData(SparkSession spark, JavaRDD<List<String>> ck, int k) {
        JavaRDD<Row> ckRDD = ck.map(x -> RowFactory.create(x.toString()));
        System.out.println("Output>>> Ck " + k + " rows " + ckRDD.count());

        StructType schema = new StructType(new StructField[]{
                new StructField("ck", DataTypes.StringType, false, Metadata.empty())
        });

        if (k == 2) {
            spark.sql("drop table if exists tmp.frequent_set_ck");
        }
        Dataset<Row> df = spark.createDataFrame(ckRDD, schema);
        df.write().mode(SaveMode.Append).saveAsTable("tmp.frequent_set_ck");
    }

    public static JavaPairRDD<List<String>, Integer> createL1(JavaRDD<Row> transactions, long rowNum, double minSupport) {
        JavaPairRDD<List<String>, Integer> c1Data = transactions.flatMapToPair(row -> {
            List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
            List<String> itemList = row.getAs(0);
            for (String item : itemList) {
                List<String> combination = new ArrayList<>(Collections.singletonList(item));
                // List<String> combination = Collections.singletonList(item);
                result.add(new Tuple2<>(combination, 1));
            }
            return result.iterator();
        });
        System.out.println("Output>>> ****** Total row number of " + c1Data.count() + " ****** ");

        // 求和
        JavaPairRDD<List<String>, Integer> c1 = c1Data.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println("Output>>> ****** Total item number of " + c1.count() + " ****** ");
        System.out.println("Output>>> C1 size: " + c1.count());

        // 过滤，生成l1
        JavaPairRDD<List<String>, Integer> l1 = c1.filter(x -> x._2.doubleValue() / rowNum >= minSupport);
        System.out.println("Output>>> L1 size: " + l1.count());

        // l1 = l1.sortByKey();
        return l1;
    }

    public static JavaRDD<Row> createTransactionsRowCk(SparkSession spark, JavaRDD<Row> transactionsWithCk, JavaRDD<List<String>> ckLast, int k) {
        // 基于transactions数据生成新的ck+1
        // 连接步
        // Broadcast<List<List<String>>> broadcastCkLast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(ckLast.collect());

        JavaRDD<Row> transactionsRowCk = transactionsWithCk.map(row -> {
            List<List<String>> result = new ArrayList<>();

            List<List<String>> itemsList = row.getAs(0);
            int itemsListLength = itemsList.size();
            for (int i = 0; i < itemsListLength; i++) {
                List<String> l1 = new ArrayList<>(itemsList.get(i));
                List<String> l1Part = l1.subList(0, k - 2);
                Collections.sort(l1Part);
                for (int j = i + 1; j < itemsListLength; j++) {
                    List<String> l2 = new ArrayList<>(itemsList.get(j));
                    List<String> l2Part = l2.subList(0, k - 2);
                    Collections.sort(l2Part);
                    if (l1Part.equals(l2Part)) {
                        // 两个list求并集，并去重复
                        List<String> combination = new ArrayList<>(l1);
                        combination.removeAll(l2); // 去除交集
                        combination.addAll(l2);  // 添加新的list
                        Collections.sort(combination);
                        result.add(combination);
                    }
                }
            }
            return RowFactory.create(result);
        }).filter(row -> ((List) row.getAs(0)).size() > 0);

        // testing
        showTransactionsOverview(spark, transactionsRowCk, k);

        return transactionsRowCk;
    }

    public static JavaPairRDD<List<String>, Integer> createLK(SparkSession spark, JavaRDD<Row> transactionsRowCk, JavaRDD<List<String>> ck, long rowNum, double minSupport, int k) {
        if ( k >= 3) {
            transactionsRowCk = transactionsRowCk.repartition(Integer.parseInt(System.getProperty("param.repartition.num", DEFAULT_PARTITIONS)) * 10);
        }
        // 计数
        JavaPairRDD<List<String>, Integer> ckData = transactionsRowCk.flatMapToPair(row -> {
            List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
            List<List<String>> rowCK = row.getAs(0);
            for (List<String> combination : rowCK) {
                result.add(new Tuple2<>(combination, 1));
            }
            return result.iterator();
        });

        ckData.cache();
        long counter = ckData.count();
        System.out.println("Output>>> C" + k + " rows: " + counter);
        if (counter == 0) {
            return ckData;
        }

        // 求和
        JavaPairRDD<List<String>, Integer> ckWithNum = ckData.reduceByKey(Integer::sum);
        System.out.println("Output>>> C" + k + " size: " + ckWithNum.count());

        // 过滤，生成lk
        JavaPairRDD<List<String>, Integer> lk = ckWithNum.filter(x -> x._2.doubleValue() / rowNum >= minSupport);
        System.out.println("Output>>> L" + k + " size: " + lk.count());

        return lk;
    }

    public static void runFindFrequentSet(SparkSession spark, Dataset<Row> df, double minSupport, String toSchemaTable) {
        // 持久化缓存 sqlDF.persist();
        // df.cache();
        long rowNum = df.count();
        System.out.println("Output>>> Total Rows: " + rowNum);

        // 设置并行数（transactions结果集小的话可以不需要设置）
        JavaRDD<Row> transactions = df.toJavaRDD();
        System.out.println(transactions.first().schema());
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());
        transactions = transactions.repartition(Integer.parseInt(System.getProperty("param.repartition.num", DEFAULT_PARTITIONS)));
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());

        // dataset预处理
        // string转换成list
        transactions = transactions.map(row -> {
            // 数据集是字符串格式（如果项目包含逗号会存在问题）
            List<String> itemList = new ArrayList<>(Arrays.asList(row.getAs(0).toString().split(",")));
            // 数据集直接使用list，强烈推荐推荐
            // List<String> itemList = new ArrayList<>(row.getAs(0));
            // 1. item去重
            Set<String> set = new HashSet<>(itemList);
            itemList = new ArrayList<>(set);
            // 2. 删除白名单
            itemList.removeAll(getWhiteList());
            // 3. 删除多空格和多null，最好使用正则
            // for (String item : itemList) {
            //     if (item.trim().equalsIgnoreCase("") || item.equalsIgnoreCase("null")) {
            //         itemList.remove(item);
            //     }
            // }
            int length = itemList.size();
            for (int i = length - 1; i >= 0; i--) {
                if (itemList.get(i).trim().equalsIgnoreCase("") || itemList.get(i).equalsIgnoreCase("null")) {
                    itemList.remove(i);
                }
            }
            // return RowFactory.create(String.join(",", itemList));
            return RowFactory.create(itemList);
        });
        // Row类型print出来是带中括号[]，类似数组，需要注意
        transactions.cache();

        System.out.println("Output>>> Show 0 Transactions Top 5");
        transactions.mapToPair(row -> new Tuple2<>(((List) row.getAs(0)).size(), row.getAs(0).toString()))
                .sortByKey(false)
                .map(x -> x._1)
                .take(5)
                .forEach(System.out::println);

        // result DataFrame
        // ArrayList<StructField> fields = new ArrayList<>();
        // Map<String, DataType> map = new LinkedHashMap<String, DataType>() {{
        //     put("frequent_set", DataTypes.StringType);
        //     put("k", DataTypes.IntegerType);
        //     put("num", DataTypes.IntegerType);
        //     put("row_num", DataTypes.LongType);
        // }};
        // for (String columnName : map.keySet()) {
        //     fields.add(DataTypes.createStructField(columnName, map.get(columnName), true));
        // }
        // StructType schema = DataTypes.createStructType(fields);

        StructType schema = new StructType(new StructField[]{
                new StructField("frequent_set", DataTypes.StringType, false, Metadata.empty()),
                new StructField("k", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("num", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("row_num", DataTypes.LongType, false, Metadata.empty()),
        });
        Dataset<Row> frequentSetDF = spark.createDataFrame(new ArrayList<>(), schema);

        // 计算第一项集
        JavaPairRDD<List<String>, Integer> l1 = createL1(transactions, rowNum, minSupport);

        // list convert javaRDD
        JavaRDD<List<String>> ck = l1.map(x -> x._1);

        ck.cache();
        saveCkData(spark, ck, 1);

        JavaRDD<Row> transactionsRowCk = transactionsConvert(transactions, ck);
        System.out.println("aaa");
        transactionsRowCk.take(1).forEach(System.out::println);
        showTransactionsOverview(spark, transactionsRowCk, 1);

        // 计算第二项集及以上
        for (int i = 2; i <= MAX_COMBINATION_NUM; i++) {
            int k = i;

            transactionsRowCk = createTransactionsRowCk(spark, transactionsRowCk, ck, k);
            JavaPairRDD<List<String>, Integer> frequentSetPairRDD = createLK(spark, transactionsRowCk, ck, rowNum, minSupport, k);
            if (frequentSetPairRDD.isEmpty()) {
                System.out.println("Output>>> L" + k + " no data, abort.");
                break;
            }

            // optimize 优化，save k项集，以便计算k+1项集;
            frequentSetPairRDD.cache();
            // ck = frequentSetPairRDD.keys().collect();
            ck = frequentSetPairRDD.map(x -> x._1);
            transactionsRowCk = transactionsFilter(transactionsRowCk, ck);

            // JavaPair convert JavaRDD<Row>
            JavaRDD<Row> frequentSetRDD = frequentSetPairRDD.map((Function<Tuple2<List<String>, Integer>, Row>) x -> {
                String a = String.join(",", x._1);
                Integer b = k;
                Integer c = x._2;
                return RowFactory.create(a, b, c, rowNum);
            });
            // JavaRDD<Row> convert Dataset
            Dataset<Row> frequentSetDS = spark.createDataFrame(frequentSetRDD, schema);
            // Dataset union
            frequentSetDF = frequentSetDF.unionAll(frequentSetDS);
        }

        // save to hive
        // save, 有时候会报权限不足的错误或警告
        // frequentSet.saveAsTextFile("/user/work/tmp/frequent_set");
        frequentSetDF.write().mode(SaveMode.Overwrite).saveAsTable(toSchemaTable);  // 如果表不存在，会默认创建，默认格式为parquet
        // result1.write().partitionBy("dt").format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.frequent_set");  // 如果表不存在，会默认创建，默认格式为parquet

        // frequentSetDF.registerTempTable("frequent_set");  // 已过时
        frequentSetDF.createOrReplaceTempView("frequent_set");
        String toSchemaTableTmp = toSchemaTable + "_tmp";
        spark.sql("drop table if exists " + toSchemaTableTmp);
        spark.sql("create table " + toSchemaTableTmp + " stored as parquet as select t.*,t.num/t.row_num as support,current_timestamp() as etl_time from frequent_set t");  // 默认textfile，需要额外指定存储格式
    }

    public static void runFindAssociationRules(SparkSession spark, double minConfidence) {
        // TODO
    }

    public static void main(String[] args) {

        // STEP-1: handle input parameters
        if (args.length < 5) {
            System.err.println("Usage: Apriori <fromSchemaTable> <fromColumn> <toSchemaTable> <minSupport> <minConfidence>");
            System.exit(1);
        }
        String fromSchemaTable = args[0];
        String fromColumn = args[1];
        String toSchemaTable = args[2];
        double minSupport = Double.parseDouble(args[3]);
        double minConfidence = Double.parseDouble(args[4]);

        // STEP-2: create a SparkSession
        SparkSession spark = HadoopUtil.createSparkSession("Apriori");
        System.out.println("==> spark all conf: " + Arrays.toString(spark.sparkContext().getConf().getAll()));
        System.out.println("==> spark all run conf: " + spark.conf().getAll());
        System.out.println("spark.executor.memory=" + spark.sparkContext().getConf().get("spark.executor.memory", "NA"));
        System.out.println("spark.yarn.executor.memoryOverhead=" + spark.sparkContext().getConf().get("spark.yarn.executor.memoryOverhead", "NA"));
        System.out.println("spark.kryoserializer.buffer.max=" + spark.conf().get("spark.kryoserializer.buffer.max", "NA"));
        System.out.println("spark.kryoserializer.buffer=" + spark.conf().get("spark.kryoserializer.buffer", "NA"));

        // spark.sparkContext().setLogLevel("WARN");

        // SparkConf sparkConf = HadoopUtil.getSparkConf();
        // sparkConf.set("spark.kryoserializer.buffer.max", "256m");
        // sparkConf.set("spark.kryoserializer.buffer", "64m");
        // SparkSession spark = SparkSession
        //         .builder()
        //         .appName("Apriori")
        //         .config(sparkConf)
        //         .enableHiveSupport()
        //         .getOrCreate();

        // STEP-3: create Dataset
        Dataset<Row> sqlDF = spark.sql("select " + fromColumn + " from " + fromSchemaTable);
        sqlDF.printSchema();
        // sqlDF.show(10);
        // sqlDF.select("items").show(10);
        // sqlDF.select(col("visitid"), col("items")).show(5);

        // 修改字段名称统一为items，方便后面固定使用。生成新的df。
        sqlDF = sqlDF.withColumnRenamed(fromColumn, STD_COLUMN);
        sqlDF.printSchema();

        runFindFrequentSet(spark, sqlDF.select(STD_COLUMN), minSupport, toSchemaTable);
        runFindAssociationRules(spark, minConfidence);

        spark.stop();
    }
}
