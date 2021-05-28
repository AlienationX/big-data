package com.ego.spark.direct;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import com.ego.HadoopUtil;

public class DStreamNetworkState {

    private static final int CHECK_POINT_DURATION_SECONDS = 1;
    private static final String CHECK_POINT_DIR = "data/updatestatebykey";

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = HadoopUtil.getSparkConf("SparkStreamingNetworkAccumulative");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(CHECK_POINT_DURATION_SECONDS));

        ssc.sparkContext().setLogLevel("WARN");

        // JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR, ...
        // https://blog.csdn.net/zangdaiyang1991/article/details/84099722
        ssc.checkpoint(CHECK_POINT_DIR);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> batchWordCounts = pairs.reduceByKey((v1, v2) -> v1 + v2);

        JavaPairDStream<String, Integer> allWordCounts = pairs.updateStateByKey(
                // private static final long serialVersionUID = -7837221857493546768L;
                (Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, value) -> {
                    // 参数values:：相当于这个batch，这个key的值可能有多个,比如(hadoop,1) (hadoop,1)传入的值就是是[1,1]


                    // Integer sum = 0;
                    // // 如果该key的state之前已经存在，那么这个key可能之前已经被统计过，否则说明这个key第一次出现
                    // if (value.isPresent()) {
                    //     sum = value.get();
                    // }
                    //
                    // // 更新state
                    // for (Integer val : valueList) {
                    //     sum += val;
                    // }
                    // return Optional.of(sum);

                    int sum = value.orElse(0);
                    for (Integer val : values) {
                        sum += val;
                    }
                    return Optional.of(sum);
                });

        // 增量推荐写入redis，就可以代替updateStateByKey全量统计
        // private static final long serialVersionUID = -4105602513005256270L;
        // Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunction = ;
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> changeWordCounts = pairs.mapWithState(
                StateSpec.function(
                        (Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>) (key, value, state) -> {
                            // curState为当前key对应的state
                            // if (value.isPresent()) {
                            //     Integer curValue = value.get();
                            //     if (state.exists()) {
                            //         state.update(state.get() + curValue);
                            //     } else {
                            //         state.update(curValue);
                            //     }
                            // }
                            // 可以删除某个key
                            // if (value.get() < 60) {
                            //     state.remove();
                            // }
                            // return new Tuple2<>(key, state.get());

                            int sum = value.orElse(0) + (state.exists() ? state.get() : 0);
                            // 更新state
                            // 每个key处理存在一个累加过程，例如一行输入hadoop hadoop hadoop的话会显示3次，(hadoop, 1) (hadoop, 2) (hadoop, 3)
                            state.update(sum);

                            return new Tuple2<>(key, sum);
                        })
        );

        // window滑动窗口一般用于统计最近的数据。每10s统计最近60s的搜索词的搜索频次，并打印出排名最靠前的5个搜索词以及出现次数
        // 窗口大小 windowDuration
        // 滑动步长 batchInterval
        JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow((v1, v2) -> v1 + v2, Durations.seconds(60), Durations.seconds(10));

        // batchWordCounts.print();
        batchWordCounts.foreachRDD(rdd -> rdd.collect().forEach(s -> System.out.println("batch  --> " + s)));
        // allWordCounts.print();
        // allWordCounts.foreachRDD(rdd -> rdd.collect().forEach(s -> System.out.println("all    --> " + s)));
        // changeWordCounts.print();
        changeWordCounts.foreachRDD(rdd -> rdd.collect().forEach(s -> System.out.println("change --> " + s)));

        windowedWordCounts.foreachRDD(rdd -> {
            System.out.println(LocalDateTime.now() + " 最近60s的词频的top5");
            rdd.mapToPair(x -> new Tuple2<>(x._2, x._1))
                    .sortByKey(false)
                    .take(5)
                    .forEach(s -> System.out.println("top 5  --> " + s));
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
