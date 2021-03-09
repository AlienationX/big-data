package com.ego.algorthms.association.spark;

import com.ego.HadoopUtil;

import org.apache.spark.SparkConf;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 优化方案：L1结果集的大小非常影响性能
 */

public class AprioriOptimize {

    private final static int MAX_COMBINATION_NUM = 10;
    private final static String STD_COLUMN = "items";

    public static List getWhiteList() {
        List<String> whiteList = new ArrayList<>();
        // return Collections.singletonList("");
        return whiteList;
    }

    public static JavaRDD<Row> transactionsFilter(List<List<String>> ck, JavaRDD<Row> transactions) {
        List<String> ckUniqueList = new ArrayList<>();
        for (List<String> lists : ck) {
            for (String list : lists) {
                if (!ckUniqueList.contains(list)) {
                    ckUniqueList.add(list);
                }
            }
        }

        return transactions.map(row -> {
            List<String> itemList = Arrays.asList(row.getAs(STD_COLUMN).toString().split(","));
            itemList.removeAll(ckUniqueList); // 去除交集
            itemList.addAll(ckUniqueList);  // 添加新的list
            return RowFactory.create(String.join(",", itemList));
        });
    }

    public static JavaPairRDD<List<String>, Integer> createL1(JavaRDD<Row> transactions, long rowNum, double minSupport) {
        JavaPairRDD<List<String>, Integer> c1Data = transactions.flatMapToPair(new PairFlatMapFunction<Row, List<String>, Integer>() {
            @Override
            public Iterator<Tuple2<List<String>, Integer>> call(Row row) throws Exception {
                List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
                String transaction = row.getAs(STD_COLUMN);

                String[] itemsList = transaction.split(",");
                for (String item : itemsList) {
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
        // c1.foreach(System.out::println);
        // 过滤，生成l1
        JavaPairRDD<List<String>, Integer> l1 = c1.filter(x -> x._2.doubleValue() / rowNum >= minSupport);
        System.out.println("Output>>> L1 size: " + l1.count());
        // l1.foreach(System.out::println);

        // l1 = l1.sortByKey();
        return l1;
    }

    // public static List<List<String>> createCK(List<List<String>> ckLast, int k) {
    //     // 连接步
    //     List<List<String>> ck = new ArrayList<>();
    //     int ckLastLength = ckLast.size();
    //     for (int i = 0; i < ckLastLength; i++) {
    //         for (int j = i + 1; j < ckLastLength; j++) {
    //             List<String> l1 = new ArrayList<>(ckLast.get(i));
    //             List<String> l1Part = l1.subList(0, k - 2);
    //             Collections.sort(l1Part);
    //             List<String> l2 = new ArrayList<>(ckLast.get(j));
    //             List<String> l2Part = l2.subList(0, k - 2);
    //             Collections.sort(l2Part);
    //             if (l1Part.equals(l2Part)) {
    //                 l1.removeAll(l2); // 去除交集
    //                 l1.addAll(l2);  // 添加新的list
    //                 ck.add(l1);
    //             }
    //         }
    //     }
    //     return ck;
    // }

    public static JavaRDD<List<String>> createCK(SparkSession spark, JavaRDD<List<String>> ckLast, int k) {
        // 连接步
        // method 1：笛卡尔积后使用filter过滤, 存在相同key的
        // ckLast.repartition(100);
        // ckLast.cache();
        // JavaRDD<List<String>> ck = ckLast.cartesian(ckLast)
        //         .filter(x -> {
        //             List<String> l1 = new ArrayList<>(x._1);
        //             List<String> l1Part = l1.subList(0, k - 2);
        //             Collections.sort(l1Part);
        //             List<String> l2 = new ArrayList<>(x._2);
        //             List<String> l2Part = l2.subList(0, k - 2);
        //             Collections.sort(l2Part);
        //             return l1Part.equals(l2Part);
        //         }).map(x -> {
        //             List<String> l1 = new ArrayList<>(x._1);
        //             List<String> l2 = new ArrayList<>(x._2);
        //             l1.removeAll(l2); // 去除交集
        //             l1.addAll(l2);  // 添加新的list
        //             Collections.sort(l1);
        //             return l1;
        //         }).distinct();

        // method 2：广播变量,双层嵌套for循环
        // Broadcast<List<List<String>>> broadcastCkLast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(ckLast.collect());
        // ckLast = ckLast.repartition(100);
        // ckLast.mapPartitions(iter -> {
        //     List<List<String>> result = new ArrayList<>();
        //     while (iter.hasNext()) {
        //         List<String> l1 = new ArrayList<>(iter.next());
        //         for (List<String> l2 : broadcastCkLast.value()) {
        //             l1.removeAll(l2); // 去除交集
        //             l1.addAll(l2);  // 添加新的list
        //             Collections.sort(l1);
        //             result.add(l1);
        //         }
        //     }
        //     return result.iterator();
        // });

        // method 3：老方法，增加index,然后双层遍历使用index
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

    public static JavaPairRDD<List<String>, Integer> createLK(JavaRDD<Row> transactions, Broadcast<List<List<String>>> combinations, long rowNum, double minSupport, int k) {
        // 计数
        // method1.使用笛卡尔积计算两个rdd
        // transactions = transactions.repartition(100);
        // JavaPairRDD<List<String>, Integer> ckData = transactions.cartesian(combinations)
        //         .filter((Tuple2<Row, List<String>> x) -> {
        //             String transaction = x._1.getAs(STD_COLUMN);
        //             return Arrays.asList(transaction.split(",")).containsAll(x._2);
        //             // }).mapToPair(x -> new Tuple2<>(x._2, 1));
        //         }).mapPartitionsToPair(iter -> {
        //             List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
        //             while (iter.hasNext()) {
        //                 Tuple2<Row, List<String>> x = iter.next();
        //                 result.add(new Tuple2<>(x._2, 1));
        //             }
        //             return result.iterator();
        //         });

        // method2.广播一个rdd进行计算
        JavaPairRDD<List<String>, Integer> ckData = transactions.flatMapToPair(x -> {
            List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
            String transaction = x.getAs(STD_COLUMN);

            List<String> itemsList = new ArrayList<>(Arrays.asList(transaction.split(",")));
            for (List<String> combination : combinations.value()) {
                if (itemsList.containsAll(combination)) {
                    result.add(new Tuple2<>(combination, 1));
                }
            }
            return result.iterator();
        });
        ckData.cache();
        System.out.println("Output>>> C" + k + " rows: " + ckData.count());

        // 下面两个的foreache的区别？
        // ckData.collect().forEach(System.out::println);
        // ckData.foreach(System.out::println);

        // 求和
        JavaPairRDD<List<String>, Integer> ckWithNum = ckData.reduceByKey(Integer::sum);
        System.out.println("Output>>> C" + k + " size: " + ckWithNum.count());
        // ck.foreach(System.out::println);
        // 过滤，生成lk
        JavaPairRDD<List<String>, Integer> lk = ckWithNum.filter(x -> x._2.doubleValue() / rowNum >= minSupport);
        System.out.println("Output>>> L" + k + " size: " + lk.count());
        // lk.foreach(System.out::println);

        return lk;
    }

    public static void runFindFrequentSet(SparkSession spark, Dataset<Row> df, double minSupport, String toSchemaTable) {
        // 持久化缓存 sqlDF.persist();
        // df.cache();
        long rowNum = df.count();
        System.out.println("Output>>> Total Rows: " + rowNum);

        // 设置并行数（transactions结果集小的话可以不需要设置）
        JavaRDD<Row> transactions = df.toJavaRDD();
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());
        transactions = transactions.repartition(60);
        System.out.println("JavaRDD<Row> transactions's partition size: " + transactions.partitions().size());
        transactions.cache();

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

        // 计算第一项集
        JavaPairRDD<List<String>, Integer> l1 = createL1(transactions, rowNum, minSupport);
        // List<List<String>> ck = l1.keys().collect();

        // list convert javaRDD
        JavaRDD<List<String>> ck = l1.map(x -> x._1);

        // 计算第二项集及以上
        for (int i = 2; i <= MAX_COMBINATION_NUM; i++) {
            ck = createCK(spark, ck, i);
            // ClassTag tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
            // Broadcast broadcastCK = spark.sparkContext().broadcast("words",tag);
            Broadcast<List<List<String>>> broadcastCK = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(ck.collect());
            JavaPairRDD<List<String>, Integer> frequentSetPairRDD = createLK(transactions, broadcastCK, rowNum, minSupport, i);
            System.out.println("Output>>> Finish L" + i);
            if (frequentSetPairRDD.isEmpty()) {
                System.out.println("Output>>> L" + i + " no data, abort.");
                break;
            }
            // frequentSetPairRDD.foreach(System.out::println);

            // save i项集，以便计算i+1项集;
            frequentSetPairRDD.cache();
            // ck = frequentSetPairRDD.keys().collect();
            ck = frequentSetPairRDD.map(x -> x._1);

            // optimize 优化，根据LK的key过滤dataset每行的数据项目，每次循环缩小遍历的dataset结果集
            // List<String> uniqueItemSet = getUniqueItemSet(List<List<String>>)
            // transactions.map(...)

            // JavaPair convert JavaRDD<Row>
            JavaRDD<Row> frequentSetRDD = frequentSetPairRDD.map((Function<Tuple2<List<String>, Integer>, Row>) x -> {
                String a = String.join(",", x._1);
                Integer b = x._2;
                return RowFactory.create(a, b, rowNum);
            });
            // JavaRDD<Row> convert Dataset
            Dataset<Row> frequentSetDS = spark.createDataFrame(frequentSetRDD, schema);
            // Dataset union
            frequentSetDF = frequentSetDF.unionAll(frequentSetDS);
        }

        System.out.println("Output>>> Finish1...");
        // save to hive
        // save, 有时候会报权限不足的错误或警告
        // frequentSet.saveAsTextFile("/user/work/tmp/frequent_set");
        frequentSetDF.write().mode(SaveMode.Overwrite).saveAsTable(toSchemaTable);  // 如果表不存在，会默认创建，默认格式为parquet
        // result1.write().partitionBy("dt").format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.frequent_set");  // 如果表不存在，会默认创建，默认格式为parquet

        System.out.println("Output>>> Finish2...");
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
        System.out.println("spark all conf: " + Arrays.toString(spark.sparkContext().getConf().getAll()));
        System.out.println("spark all conf: " + spark.conf().getAll());
        System.out.println(spark.sparkContext().getConf().get("spark.executor.memory", "NA"));
        System.out.println("spark.kryoserializer.buffer.max=" + spark.conf().get("spark.kryoserializer.buffer.max", "NA"));
        System.out.println("spark.kryoserializer.buffer=" + spark.conf().get("spark.kryoserializer.buffer", "NA"));

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
