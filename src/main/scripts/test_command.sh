#!/usr/bin/env bash


hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.hive.HiveJDBC

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.hive.ImpalaJDBC

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.datasync.HiveToEs -libjars elasticsearch-hadoop-7.6.2.jar,hive-hcatalog-core-1.1.0-cdh5.16.2.jar customers customers/table

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.WordCountES -libjars elasticsearch-hadoop-7.6.2.jar tmp/samples.txt medical/wordcount

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.sql.GroupBy a b


########################################################################################################################
# execute hadoop jar with libjars
HIVE_LIB="/opt/cloudera/parcels/CDH/lib/hive/lib"
HCATALOG_LIB="/opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog"

hive_jars=`ls -R ${HIVE_LIB}| grep jar`
hive_jars=${HIVE_LIB}/`echo ${hive_jars} | sed "s# #,${HIVE_LIB}/#g"`
echo ${hive_jars}

hcatalog_jars=`ls -R ${HCATALOG_LIB}| grep jar`
hcatalog_jars=${HCATALOG_LIB}/`echo ${hcatalog_jars} | sed "s# #,${HCATALOG_LIB}/#g"`
echo ${hcatalog_jars}

all_jars=${hive_jars},${hcatalog_jars}

echo ${all_jars}
hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.sql.GroupBy -libjars ${all_jars} a b
