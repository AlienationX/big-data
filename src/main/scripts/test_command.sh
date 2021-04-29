#!/usr/bin/env bash


hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.hive.HiveJDBC

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.hive.ImpalaJDBC

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.datasync.HiveToEs -libjars elasticsearch-hadoop-7.6.2.jar,hive-hcatalog-core-1.1.0-cdh5.16.2.jar customers customers/table

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.WordCountES -libjars elasticsearch-hadoop-7.6.2.jar tmp/samples.txt medical/wordcount

hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.sql.Aggregation a b


spark2-submit \
--master yarn \
--deploy-mode cluster \
--name "Find Association Rules" \
--driver-memory 1g \
--executor-memory 10g \
--queue root.flow \
--class com.ego.algorthms.association.spark.FindAssociationRules \
bigdata-1.0-SNAPSHOT.jar 0.1 0.5

spark2-submit \
--master yarn \
--deploy-mode client \
--name "Find Association Rules" \
--driver-memory 2g \
--executor-memory 6g \
--queue root.workflow \
--class com.ego.algorthms.association.spark.FindAssociationRules \
bigdata-1.0-SNAPSHOT.jar 0.02857 1

spark2-submit \
--master yarn \
--deploy-mode client \
--name "Aprioir" \
--driver-memory 2g \
--executor-memory 6g \
--queue root.workflow \
--class com.ego.algorthms.association.spark.Apriori \
bigdata-1.0-SNAPSHOT.jar tmp.transactions_zy clientids tmp.transactions_zy_set 0.02857 1

time spark2-submit \
--master yarn \
--deploy-mode cluster \
--name "Aprioir" \
--driver-memory 2g \
--executor-memory 4g \
--num-executors 4 \
--queue root.workflow \
--class com.ego.algorthms.association.spark.Apriori \
bigdata-1.0-SNAPSHOT.jar tmp.transactions_zy clientids tmp.transactions_zy_set 0.0042857 1

time spark2-submit \
--master yarn \
--deploy-mode client \
--name "HiveToClickhouse" \
--driver-memory 8g \
--executor-memory 8g \
--queue root.workflow \
--jars clickhouse-native-jdbc-shaded-2.3-stable.jar \
--class com.ego.spark.DataSyncHiveSqlToClickHouse \
bigdata-1.0-SNAPSHOT.jar "select * from medical.dwb_master_info t where t.project='funan' and t.etl_source='A01'" test.dwb_master_info


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
hadoop jar bigdata-1.0-SNAPSHOT-assembly.jar com.ego.mapreduce.sql.Aggregation -libjars ${all_jars} a b
