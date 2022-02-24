package com.test

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object TransformLogFile {

    // case class 类(param1：type1 )需要放在函数外面，作为成员变量。
    case class SchemaLog1(id: Long, query_id: String, start_time: String, user: String, query_str: String)

    case class SchemaLog2(query_id: String, end_time: String, duration: String, duration_raw: String)

    case class Employee(name: String, age: Int, salary: Long)

    def main(args: Array[String]): Unit = {
        //Logger.getLogger("org").setLevel(Level.WARN)

        val spark = SparkSession
                .builder()
                .appName(this.getClass.getSimpleName)
                .master("local")
                .getOrCreate()
        // 导入隐士转换，必须引入才能使用rdd.toDF() rdd.toDS()
        import spark.implicits._

        val sc = spark.sparkContext
        sc.setLogLevel("WARN")

        val data = new util.ArrayList[Row]()
        val broadcastData = sc.broadcast(data)

        val accum = spark.sparkContext.longAccumulator("Group id")
        val path = "/user/work/flume/hive_log/2022-01-05.log"
        val logDF = spark.read.text(path)  // 返回字段为value的DF
        logDF.show()

        val logRDD: RDD[String] = sc.textFile(path).coalesce(1)
        logRDD.cache()
        println("partition Num: " + logRDD.getNumPartitions)

        val logDetailDF1 = logRDD.map(line => {
            println("Raw ===> " + line)
            if (line.contains("INFO") || line.contains("WARN") || line.contains("ERROR")) {
                accum.add(1)
            }
            // Compiling command / Executing command / Completed executing command
            if (line.contains("Compiling command")) {
                val queryId = "queryId=(.*)\\):".r.findFirstMatchIn(line).iterator.next().group(1)
                val startTime = line.substring(0, 23)
                val user = "queryId=(.*)_20".r.findFirstMatchIn(line).iterator.next().group(1)
                val queryStr = line.substring(line.indexOf("): ") + 3)
                SchemaLog1(accum.value, queryId, startTime, user, queryStr)
            } else if (line.contains("Executing command")) {
                SchemaLog1(accum.value, "Executing", null, null, line) // 换行sql的前一行是否需要保留并处理?
            } else if (line.contains("INFO")) {
                SchemaLog1(accum.value, "INFO", null, null, null) // 普通INFO信息可以过滤掉
            } else if (line.contains("WARN") || line.contains("ERROR")) {
                SchemaLog1(accum.value, "WARN or ERROR", null, null, null)
            } else {
                SchemaLog1(accum.value, null, null, null, line) // 存在换行数据，换行数据包括Compiling command的换行sql、和Executing command的换行sql、和报错信息，共3种
            }
        }).toDF().cache()
        logDetailDF1.foreach(row => println(row))
        //logDetailDF1.cache()
        logDetailDF1.createOrReplaceTempView("log1_detail")
        val logDF1 = spark.sql("select t.id, max(t.query_id) as query_id, max(t.start_time) as start_time, max(t.user) as user, concat_ws(' ', collect_list(trim(t.query_str))) as query_str " +
                "from log1_detail t " +
                "where t.id not in (select a.id from log1_detail a where a.query_id in ('Executing','INFO','WARN or ERROR') group by a.id) " +
                "group by t.id")
        logDF1.createOrReplaceTempView("log1")

        val logDF2 = logRDD.map(line => {
            if (line.contains("Completed compiling command")) {
                val queryId = "queryId=(.*)\\);".r.findFirstMatchIn(line).iterator.next().group(1)
                val endTime = line.substring(0, 23)
                val duration = "Time taken: (.*) seconds".r.findFirstMatchIn(line).iterator.next().group(1)
                val durationRaw = "Time taken: (.*)".r.findFirstMatchIn(line).iterator.next().group(1)
                // Row(queryId, endTime, endTimeRaw)  // 封装成Row类
                SchemaLog2(queryId, endTime, duration, durationRaw)
            } else {
                SchemaLog2("", "", "", "")
            }
        }).filter(row => !row.query_id.equals("")).toDF().cache()
        logDF2.show()
        logDF2.createOrReplaceTempView("log2")

        spark.sql("select t.id, t.query_id, t.start_time, s.end_time, s.duration, t.user, t.query_str from log1 t left join log2 s on t.query_id=s.query_id").show()

        logDF1.join(logDF2, Seq("query_id"), "left").foreach(row=>println(row))


        val schema = StructType(List(
            StructField("query_id", StringType, nullable = true),
            StructField("start_time", StringType, nullable = true),
            StructField("end_time", StringType, nullable = true),
            StructField("user", StringType, nullable = true),
            StructField("query_str", StringType, nullable = true)
        ))
        spark.createDataFrame(data, schema).show()

        //第一种：通过Seq生成
        val df = spark.createDataFrame(Seq(
            ("ming", 20, 15552211521L),
            ("gong", 19, 13287994007L),
            ("zhi", 21, 15552211523L)
        )).toDF("name", "age", "phone")
        df.show()

        //第二种：rdd转df （Employee类必须写在函数外，作为成员变量）
        spark.sparkContext.parallelize(Seq(
            Employee("ming", 20, 15552211521L),
            Employee("gong", 19, 13287994007L),
            Employee("zhi", 21, 15552211523L)
        )).toDF().show()

        spark.stop()
    }
}
