// package com.ego.spark;
//
//
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.hbase.HBaseConfiguration;
// import org.apache.hadoop.hbase.client.Result;
// import org.apache.hadoop.hbase.client.Scan;
// import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
// import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
// import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
// import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
// import org.apache.hadoop.hbase.util.Base64;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.sql.SparkSession;
//
// import com.ego.HadoopUtil;
//
// import java.io.IOException;
//
// public class SparkHBase {
//
//     public static void main(String[] args) throws IOException {
//         SparkSession spark = HadoopUtil.createSparkSession("Spark HBase");
//         JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//         Configuration conf = HBaseConfiguration.create();
//         conf.set("hbase.zookeeper.quorum", "hadoop-prod03:2181,hadoop-prod04:2181,hadoop-prod08:2181");
//
//         Scan scan = new Scan();
//         // 将scan编码
//         ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//         String scanToString = Base64.encodeBytes(proto.toByteArray());
//         // 表名
//         String tableName = "Contacts";
//         conf.set(TableInputFormat.INPUT_TABLE, tableName);
//         conf.set(TableInputFormat.SCAN, scanToString);
//
//         // 将HBase数据转成RDD
//         JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
//                 ImmutableBytesWritable.class, Result.class);
//
//         System.out.println(hbaseRdd);
//     }
// }
