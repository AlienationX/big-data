package com.test

import org.apache.spark.sql.SparkSession

object SparkSqlWithHive {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "work")
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin")
        val spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("Spark SQL Example")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate()
        //val sqlDF = spark.sql("select visitid, orgid, orgname, clientid, clientname, doctorid, doctorname, visitdate, diseasename from medical.dwb_master_info t limit 20")
        val sqlDF = spark.sql("show databases")
        sqlDF.printSchema()
        sqlDF.show()
    }
}