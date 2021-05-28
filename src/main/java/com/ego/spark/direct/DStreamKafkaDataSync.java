package com.ego.spark.direct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;

import com.ego.HadoopUtil;

public class DStreamKafkaDataSync {

    private static final int CHECK_POINT_DURATION_SECONDS = 1;
    private static final String CHECK_POINT_DIR = "data/updatestatebykey";

    public static String parseValue(Object inputValue){
        if (inputValue == null) {
            return "null";
        }
        // Integer、Double、null
        String value = inputValue.toString();
        String dataType = inputValue.getClass().getSimpleName();
        if ("String".equals(dataType)) {
            value = "'" + inputValue + "'";
        }
        return value;
    }

    public static String parseSql(String jsonString) throws IOException {
        // 自己解析binlog太麻烦了，各种问题，不知道有没有现成的工具？

        ObjectMapper mapper = new ObjectMapper();
        // Map jsonMap = mapper.readValue(jsonString, Map.class);
        Map jsonMap = mapper.readValue(jsonString, LinkedHashMap.class);
        System.out.println(jsonMap);

        String sql = "";
        String action = jsonMap.get("action").toString();
        String table = jsonMap.get("schema") + "." + jsonMap.get("table");
        List<String> pkList = (List<String>) jsonMap.get("table_pk");
        if ("insert".equals(action)) {
            List<String> data = new ArrayList<>();
            for (Map<String, Object> map : (List<Map<String, Object>>) jsonMap.get("data")) {
                List<String> rowList = new ArrayList();

                Map<String, Object> rowMap = (Map<String, Object>) map.get("values");
                for (Object val : rowMap.values()) {
                    rowList.add(parseValue(val));
                }
                data.add("(" + String.join(", ", rowList) + ")");
            }
            sql = "insert into " + table + " values " + String.join(", ", data);
        }
        // update，delete强烈推荐根据主键修改或删除数据
        else if ("update".equals(action)) {
            List<String> updateRowSqlList = new ArrayList<>();
            for (Map<String, Object> map : (List<Map<String, Object>>) jsonMap.get("data")) {
                List<String> rowList = new ArrayList();
                List<String> whereList = new ArrayList<>();

                Map<String, Object> rowMap = (Map<String, Object>) map.get("after_values");
                for (Object key : rowMap.keySet()) {
                    if (pkList.contains(key)) {
                        whereList.add(key + "=" +  parseValue(rowMap.get(key)));
                    } else {
                        rowList.add(key + "=" + parseValue(rowMap.get(key)));
                    }
                }
                String updateRowSql = "update " + table
                        + " set " + String.join(", ", rowList)
                        + " where " + String.join(" and ", whereList);
                updateRowSqlList.add(updateRowSql);
            }
            sql = String.join(";\n", updateRowSqlList);
        } else if ("delete".equals(action)) {
            Set<String> whereSet = new HashSet<>();
            for (Map<String, Object> map : (List<Map<String, Object>>) jsonMap.get("data")) {
                List<String> whereList = new ArrayList<>();

                Map<String, Object> rowMap = (Map<String, Object>) map.get("values");
                for (Object key : rowMap.keySet()) {
                    if (pkList.contains(key)) {
                        whereList.add(key + "=" + parseValue(rowMap.get(key)));
                    }
                }
                whereSet.add("(" + String.join(" and ", whereList) + ")");
            }
            sql = "delete from " + table + " where " + String.join(" and ", whereSet);
        }
        return sql;
    }

    public static void main(String[] args) throws Exception {

        HadoopUtil.setEnvironment();

        // StreamingExamples.setStreamingLogLevels();

        String brokers = "hadoop-prod03:9092";
        String groupId = "test";
        String topics = "data_sync.test.test4,data_sync.test.student";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingKafkaWordCount").setMaster("local[*]");
        // 处理完当前批次的数据才会终止程序
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");

        // JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        // ssc.sparkContext().setLogLevel("WARN");

        // 读取checkpoint容错
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR , (Function0<JavaStreamingContext>) () -> {
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(CHECK_POINT_DURATION_SECONDS));
            jssc.sparkContext().setLogLevel("WARN");
            jssc.checkpoint(CHECK_POINT_DIR);
            return jssc;
        });

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<TopicPartition,Long> partitionsAndOffset = new HashMap<>();//  配置对应的主题分区的offset，从指定offset消费
        partitionsAndOffset.put(new TopicPartition("test",0),0L);
        partitionsAndOffset.put(new TopicPartition("test",1),10L);
        partitionsAndOffset.put(new TopicPartition("test",2),330L);

        // Create direct kafka stream with brokers and topics
        // 自动读取上次未消费的数据，如何记录和提交offset的？
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(), // 分区策略
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)  // 消费者策略 方式一 默认从每个分区的latest消费
                // ConsumerStrategies.Subscribe(topicsSet,kafkaParams,partitionsAndOffset) //消费者策略 方式二 从每个分区指定的offset开始消费
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        lines.foreachRDD(rdd -> {
            rdd.collect().forEach(System.out::println);

            // do something, save to mysql、kudu、hbase、solr/es.
            // 不推荐使用mapPartitions，分区会影响执行顺序
            rdd.map(x -> parseSql(x))
                    .collect()
                    .forEach(System.out::println);
        });

        // Start the computation
        ssc.start();
        ssc.awaitTermination();
    }

}
