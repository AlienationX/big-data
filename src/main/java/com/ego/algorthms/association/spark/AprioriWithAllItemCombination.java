package com.ego.algorthms.association.spark;

import com.ego.HadoopUtil;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * 基于所有item进行组合
 * 两两组合公式：N*(N-1)/2
 *
 * -- 6w的一项集项目就有20亿的组合，会生成实际未产生的项集，强烈不推荐
 * -- 63607*63606/2 = 2022893421
 */

public class AprioriWithAllItemCombination {

    private final static int MAX_COMBINATION_NUM = 10;
    private final static String STD_COLUMN = "items";

    public static List<String> getWhiteList() {
        List<String> whiteList = new ArrayList<>();
        whiteList.add("");
        whiteList.add("null");
        whiteList.add("NULL");
        return whiteList;
    }

    public static JavaRDD<Row> transactionsFilter(JavaRDD<List<String>> ck, JavaRDD<Row> transactions) {
        List<String> ckUniqueList = new ArrayList<>();
        for (List<String> lists : ck.collect()) {
            for (String list : lists) {
                if (!ckUniqueList.contains(list)) {
                    ckUniqueList.add(list);
                }
            }
        }

        return transactions.map(row -> {
            // 两个list求交集
            // List<String> itemList = row.getList(0);
            List<String> itemList = new ArrayList<>(Arrays.asList(row.getAs(0).toString().split(",")));
            itemList.retainAll(ckUniqueList);
            // List<String> intersection = itemList.stream().filter(item -> ckUniqueList.contains(item)).collect(toList());
            // return RowFactory.create(itemList);
            return RowFactory.create(String.join(",", itemList));
        });
    }

    public static void showTransactionsOverview(JavaRDD<Row> transactions, SparkSession spark, int k) {
        // 内置的累加器有三种，LongAccumulator、DoubleAccumulator、CollectionAccumulator
        // LongAccumulator: 数值型累加
        LongAccumulator accumulatorNum = spark.sparkContext().longAccumulator("accumulatorNum");
        accumulatorNum.setValue(0);

        // JavaPairRDD<Integer, List<String>> integerListJavaPairRDD =
        transactions.mapToPair(row -> {
            accumulatorNum.add(1);
            return new Tuple2<>(row.getAs(0).toString().split(",").length, row.getAs(0).toString());
        }).cache()
                .sortByKey(false)
                .map(x -> x._1)
                .take(5)
                .forEach(System.out::println);
        System.out.println("Output>>> Show " + k + " Transactions Top 5");
        System.out.println("Output>>> Show " + k + " Transactions Count " + transactions.count());
        System.out.println("Output>>> Show " + k + " Transactions LongAccumulator " + accumulatorNum.value());
    }

    public static void saveCkData(JavaRDD<List<String>> ck, SparkSession spark, int k) {
        JavaRDD<Row> ckRDD = ck.map(x -> RowFactory.create(x.toString()));
        System.out.println("Output>>> Ck " + k + " rows " + ckRDD.count());

        StructType schema = new StructType(new StructField[]{
                new StructField("ck", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(ckRDD, schema);
        df.write().mode(SaveMode.Append).saveAsTable("tmp.frequent_set_ck");
    }

    public static JavaPairRDD<List<String>, Integer> createL1(JavaRDD<Row> transactions, long rowNum, double minSupport) {
        JavaPairRDD<List<String>, Integer> c1Data = transactions.flatMapToPair(new PairFlatMapFunction<Row, List<String>, Integer>() {
            @Override
            public Iterator<Tuple2<List<String>, Integer>> call(Row row) throws Exception {
                List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
                // List<String> itemList = row.getList(0);
                String transaction = row.getAs(0);
                String[] itemList = transaction.split(",");
                for (String item : itemList) {
                    List<String> combination = new ArrayList<>(Collections.singletonList(item));
                    // List<String> combination = Collections.singletonList(item);
                    result.add(new Tuple2<>(combination, 1));
                }
                return result.iterator();
            }
        });
        System.out.println("Output>>> ****** Total row number of " + c1Data.count() + " ****** ");

        // 求和
        JavaPairRDD<List<String>, Integer> c1 = c1Data.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("Output>>> ****** Total item number of " + c1.count() + " ****** ");
        System.out.println("Output>>> C1 size: " + c1.count());

        // 过滤，生成l1
        JavaPairRDD<List<String>, Integer> l1 = c1.filter(x -> x._2.doubleValue() / rowNum >= minSupport);
        System.out.println("Output>>> L1 size: " + l1.count());

        // l1 = l1.sortByKey();
        return l1;
    }

    public static JavaRDD<List<String>> createCK(SparkSession spark, JavaRDD<List<String>> ckLast, int k) {
        // 基于ck数据生成新的ck+1
        // 连接步
        // 增加index为值，并key、value互换成(index, List<String>)
        JavaPairRDD<Long, List<String>> ckLastPairRDD = ckLast.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2, x._1));
        Long ckLength = ckLastPairRDD.count();

        // 广播map
        Map<Long, List<String>> ckLastPairRDDMap = new HashMap<>(ckLastPairRDD.collectAsMap());
        Broadcast<Map<Long, List<String>>> broadcastCK = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(ckLastPairRDDMap);

        JavaRDD<List<String>> ck = ckLastPairRDD.flatMap(x -> {
            List<List<String>> result = new ArrayList<>();

            Long i = x._1;
            List<String> l1 = new ArrayList<>(x._2);
            List<String> l1Part = l1.subList(0, k - 2);
            Collections.sort(l1Part);
            for (Long j = i + 1; j < ckLength; j++) {
                List<String> l2 = new ArrayList<>(broadcastCK.value().get(j));
                List<String> l2Part = l2.subList(0, k - 2);
                Collections.sort(l2Part);
                if (l1Part.equals(l2Part)) {
                    // 两个list求并集，并去重复
                    List<String> combination = new ArrayList<>(l1);
                    combination.removeAll(l2); // 去除交集
                    combination.addAll(l2);  // 添加新的list
                    result.add(combination);
                }
            }
            return result.iterator();
        });

        System.out.println("Output>>> c" + k + " combinations: " + ck.count());

        return ck;
    }

    public static JavaPairRDD<List<String>, Integer> createLK(SparkSession spark, JavaRDD<Row> transactions, JavaRDD<List<String>> ck, long rowNum, double minSupport, int k) {
        // 计数

        // 广播一个rdd进行计算，广播ck
        // ClassTag tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        // Broadcast broadcastCK = spark.sparkContext().broadcast("words",tag);
        // Broadcast<List<List<String>>> broadcastCK = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(ck.collect());
        //
        // JavaPairRDD<List<String>, Integer> ckData = transactions.flatMapToPair(row -> {
        //     List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
        //     String transaction = row.getAs(0);
        //
        //     List<String> itemList = new ArrayList<>(Arrays.asList(transaction.split(",")));
        //     for (List<String> combination : broadcastCK.value()) {
        //         // 瓶颈
        //         if (itemList.containsAll(combination)) {
        //             result.add(new Tuple2<>(combination, 1));
        //         }
        //     }
        //     return result.iterator();
        // });

        // 广播一个rdd进行计算，广播transactions
        Broadcast<List<Row>> broadcastTransactions = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(transactions.collect());

        // ck = ck.repartition(280);
        JavaPairRDD<List<String>, Integer> ckData = ck.flatMapToPair(x -> {
            List<Tuple2<List<String>, Integer>> result = new ArrayList<>();

            for (Row row : broadcastTransactions.value()) {
                List<String> itemList = new ArrayList<>(Arrays.asList(row.getAs(0).toString().split(",")));
                // 瓶颈
                if (itemList.containsAll(x)) {
                    result.add(new Tuple2<>(x, 1));
                }
            }
            return result.iterator();
        });
        ckData.cache();
        System.out.println("Output>>> C" + k + " rows: " + ckData.count());

        // 下面两个的foreach的区别？
        // ckData.collect().forEach(System.out::println);
        // ckData.foreach(System.out::println);

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
        transactions = transactions.repartition(32);
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());

        // dataset预处理
        // string转换成list，但是getList报错：java.util.ArrayList cannot be cast to scala.collection.Seq
        transactions = transactions.map(row -> {
            List<String> itemList = new ArrayList<>(Arrays.asList(row.getAs(0).toString().split(",")));
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
            // return RowFactory.create(itemList);
            return RowFactory.create(String.join(",", itemList));
        });
        // Row类型print出来是带中括号[]，类似数组，需要注意
        // transactions.take(2).forEach(System.out::println);
        transactions.cache();

        // 倒序显示项目数最多的5条
        showTransactionsOverview(transactions, spark, 1);

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
        // List<List<String>> ck = l1.keys().collect();

        // list convert javaRDD
        JavaRDD<List<String>> ck = l1.map(x -> x._1);

        ck.cache();
        saveCkData(ck, spark, 1);

        // 计算第二项集及以上
        for (int i = 2; i <= MAX_COMBINATION_NUM; i++) {
            int k = i;
            // optimize 优化，根据LK的key过滤dataset每行的数据项目，每次循环缩小遍历的dataset结果集，提高createLK的containsAll效率
            transactions = transactionsFilter(ck, transactions)
                    .filter(row -> row.getAs(0).toString().split(",").length > k);
            // 倒序显示项目数最多的5条
            showTransactionsOverview(transactions, spark, k);

            ck = createCK(spark, ck, k);

            ck.cache();
            saveCkData(ck, spark, k);

            JavaPairRDD<List<String>, Integer> frequentSetPairRDD = createLK(spark, transactions, ck, rowNum, minSupport, k);
            if (frequentSetPairRDD.isEmpty()) {
                System.out.println("Output>>> L" + k + " no data, abort.");
                break;
            }

            // save k项集，以便计算k+1项集;
            frequentSetPairRDD.cache();
            // ck = frequentSetPairRDD.keys().collect();
            ck = frequentSetPairRDD.map(x -> x._1);

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
