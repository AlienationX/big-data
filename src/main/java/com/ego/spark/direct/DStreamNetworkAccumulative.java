package com.ego.spark.direct;

import com.ego.HadoopUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

public class DStreamNetworkAccumulative {

    private static final int CHECK_POINT_DURATION_SECONDS = 5;
    private static final String CHECK_POINT_DIR = "data/updatestatebykey";

    public static long getTime() {
        // 计算当前时间距离次日凌晨的时长(毫秒数)
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime deadLine = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth() + 1, 0, 0, 0,0);
        // long milliSeconds = deadLine.toInstant(ZoneOffset.of("+8")).toEpochMilli() - now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        long milliSeconds = deadLine.toInstant(ZoneOffset.of("+8")).toEpochMilli() - now.toInstant(ZoneOffset.of("+0")).toEpochMilli();
        System.out.println(now);
        System.out.println(deadLine);
        System.out.println("ms: "+ milliSeconds);
        // 30000ms=30s
        return 30000;
    }

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = HadoopUtil.getSparkConf("SparkStreamingNetworkAccumulative");

        while (true) {
            JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(CHECK_POINT_DURATION_SECONDS));

            ssc.sparkContext().setLogLevel("WARN");

            ssc.checkpoint(CHECK_POINT_DIR);

            JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
            JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

            // 应该使用mapWithState增量写入redis
            JavaPairDStream<String, Integer> allWordCounts = pairs.updateStateByKey(
                    // private static final long serialVersionUID = -7837221857493546768L;
                    (Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, value) -> {
                        // 参数values:：相当于这个batch，这个key的值可能有多个,比如(hadoop,1) (hadoop,1)传入的值就是是[1,1]
                        int sum = value.orElse(0);
                        for (Integer val : values) {
                            sum += val;
                        }
                        return Optional.of(sum);
                    });

            allWordCounts.print();

            ssc.start();
            ssc.awaitTerminationOrTimeout(getTime());  // 超时自动重启并重新设置超时时长
            ssc.stop();  // close就是调用的stop，还有stop(boolean)、stop(boolean, boolean)的区别，为什么推荐使用stop(false, true)?
        }
    }
}
