// package com.ego.spark;
//
// import groovy.sql.Sql;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.ml.Transformer;
// import org.apache.spark.sql.DataFrame;
// import org.apache.spark.sql.DataFrameReader;
// import org.apache.spark.sql.DataFrameWriter;
// import org.apache.spark.sql.SQLContext;
// import org.apache.spark.sql.hive.HiveContext;
// import org.jpmml.evaluator.Evaluator;
// import org.jpmml.evaluator.spark.EvaluatorUtil;
// import org.jpmml.evaluator.spark.TransformerBuilder;
//
// import com.asiainfodata.utils.EnvironmentConfig;
// import scala.Tuple2;
//
// import java.util.Arrays;
// import java.util.Map;
// import java.util.Set;
//
// public class SVMEvaluationSparkExample {
//
//
//     public static void main(String[] args) throws Exception {
//         if (EnvironmentConfig.isLocal()){
//             System.setProperty("HADOOP_USER_NAME", "work");
//             System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");
//         }
//
//         // SparkConf conf = new SparkConf();
//         // conf.setAppName("SVMEvaluationSparkExample");
//         // conf.setMaster("local");
//         // EnvironmentConfig.setHadoopConfigFile(conf);
//         SparkConf conf = EnvironmentConfig.setHadoopConfigFile();
//         System.out.println("FLAG=" + Arrays.toString(conf.getAll()));
//
//         JavaSparkContext sparkContext = new JavaSparkContext(conf);
//         SQLContext sqlContext = new HiveContext(sparkContext);
//         // sparkContext.hadoopConfiguration() 并不等于 sparkConf，需要使用sparkConf
//         for (Tuple2<String, String> c : conf.getAll()) {
//             System.out.println("SSS: "+ c._1 + "=" + c._2);
//             sqlContext.setConf(c._1, c._2);
//         }
//         // sqlContext.setConf("hive.metastore.uris", "thrift://hadoop-prod01:9083");
//         // sqlContext.setConf("hive.metastore.warehouse.dir", "/user/hive/warehouse");
//         System.out.println("Maps: " + sqlContext.getAllConfs());
//         // Map<String, String> m = scala.collection.JavaConverters.mapAsJavaMapConverter(sqlContext.getAllConfs()).asJava();
//         scala.collection.JavaConverters.mapAsJavaMapConverter(sqlContext.getAllConfs()).asJava().forEach((key, value) -> {
//             System.out.println("TTT: " + key + ":" + value);
//         });
//         sqlContext.sql("show databases").show();
//         DataFrame df = sqlContext.sql("SELECT * FROM medical.dim_date limit 5");
//         df.show();
//
//         if(args.length != 3){
//             // /tmp/data-mining/model/FraudDetectionModel.pmml
//             // /tmp/data-mining/data
//             // /tmp/data-mining/target
//             System.err.println("Usage: java " + SVMEvaluationSparkExample.class.getName() + " <PMML file> <Input file> <Output directory>");
//             System.exit(-1);
//         }
//
//         /**
//          * 根据pmml文件，构建模型
//          */
//         Configuration config = new Configuration();
//         config.addResource("shangrao/core-site.xml");
//         config.addResource("shangrao/hdfs-site.xml");
//         config.addResource("shangrao/yarn-site.xml");
//         config.addResource("shangrao/hive-site.xml");
//
//         FileSystem fs = FileSystem.get(config);
//         Evaluator evaluator = EvaluatorUtil.createEvaluator(fs.open(new Path(args[0])));
//
//         TransformerBuilder modelBuilder = new TransformerBuilder(evaluator)
//                 .withTargetCols()
//                 .withOutputCols()
//                 .exploded(true);
//
//         Transformer transformer = modelBuilder.build();
//
//         /**
//          * 利用DataFrameReader从原始数据中构造 DataFrame对象
//          * 需要原始数据包含列名
//          */
//         // SparkConf conf = new SparkConf();
//         // try(JavaSparkContext sparkContext = new JavaSparkContext(conf)){
//         //
//         //     SQLContext sqlContext = new SQLContext(sparkContext);
//         //     DataFrame df = sqlContext.sql("SELECT * FROM medical.dim_date limit 5");
//         //     df.show();
//         //
//         //     DataFrameReader reader = sqlContext.read()
//         //             .format("com.databricks.spark.csv")
//         //             .option("header", "true")
//         //             .option("inferSchema", "true");
//         //     DataFrame dataFrame = reader.load(args[1]);// 输入数据需要包含列名
//         //
//         //     /**
//         //      * 使用模型进行预测
//         //      */
//         //     // dataFrame = transformer.transform(dataFrame);
//         //
//         //     /**
//         //      * 写入数据
//         //      */
//         //     DataFrameWriter writer = dataFrame.write()
//         //             .format("com.databricks.spark.csv")
//         //             .option("header", "true");
//         //
//         //     writer.save(args[2]);
//         // }
//
//         // sparkContext.stop();
//     }
// }