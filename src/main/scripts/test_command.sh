#!/usr/bin/env bash


hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.hive.HiveJDBC

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.hive.ImpalaJDBC

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.datasync.HiveToEs -libjars elasticsearch-hadoop-7.6.2.jar,hive-hcatalog-core-1.1.0-cdh5.16.2.jar customers customers/table

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.WordCountES -libjars elasticsearch-hadoop-7.6.2.jar tmp/samples.txt medical/wordcount

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.sql.GroupBy a b
