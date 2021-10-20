package com.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("WordCountTest").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)
        val lineRdd: RDD[String] = sc.parallelize(List("Java", "Python", "Java", "C#", "Java", "Scala", "Shell", "JavaScript", "Python"))
        val wordAndOne: RDD[(String, Int)] = lineRdd.map(line => {
            (line, 1)
        })
        val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
        val resultArray: Array[(String, Int)] = reduced.collect()
        println(resultArray)
        println(resultArray.toList)
        sc.stop()
    }
}
