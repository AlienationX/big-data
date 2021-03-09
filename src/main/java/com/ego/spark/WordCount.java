package com.ego.spark;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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

        List<Integer> inputData = new ArrayList<>();
        inputData.add(10);
        inputData.add(90);
        inputData.add(20);
        JavaRDD<Integer> myRdd = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(inputData);
        Integer result = myRdd.reduce(Integer::sum);
        System.out.println(result);

        spark.stop();
    }

    private static void runHelloWorld(SparkSession spark) {
        JavaRDD<String> lines = spark.read().textFile("file:///E:/Codes/Java/big-data/data/word.txt").javaRDD();
        // JavaRDD<String> lines = spark.read().textFile("hdfs:///user/work/tmp/input_wordcount").javaRDD();
        lines.persist(StorageLevel.DISK_ONLY());
        System.out.println(lines.collect());

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // map & reduce
        // flatMap & mapToPair
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println(words.collect());

        // STEP-1: map
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        // JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
        //     @Override
        //     public Tuple2<String, Integer> call(String s) throws Exception {
        //         return new Tuple2<>(s, 1);
        //     }
        // });
        // lambda
        // JavaPairRDD<String, Integer> ones = words.mapToPair((PairFunction<String, String, Integer>) s -> {
        //     return new Tuple2<>(s, 1);
        // });
        // JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        // STEP-2: reduce
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);
        System.out.println(counts.collect());

        List<Tuple2<String, Integer>> output = counts.filter(s -> !"".equals(s._1())).collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1 + ": " + tuple._2);
        }


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // flatMapToPair, no reduceByKey
        JavaPairRDD<String, Integer> wordPairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> output = new ArrayList<>();
                String[] line = s.split(" ");
                for (String value : line) {
                    if ("".equals(value)) {
                        continue;
                    }
                    Tuple2<String, Integer> tuple = new Tuple2<>(value, 1);
                    output.add(tuple);
                }
                return output.iterator();
            }
        });
        JavaPairRDD<String, Integer> wordByKey = wordPairRDD.reduceByKey(Integer::sum);
        System.out.println("------------");
        wordByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ": " + tuple2._2);
            }
        });


        // JavaPairRDD convert JavaRDD
        JavaRDD<String> javaRDD = wordPairRDD.map(new Function<Tuple2<String, Integer>, String>() {
            public String call(Tuple2<String, Integer> t) throws Exception {
                return t._1 + ": " + t._2;
            }
        });
        System.out.println(javaRDD.collect());

    }

}