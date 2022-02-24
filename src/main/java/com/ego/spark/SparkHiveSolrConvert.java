package com.ego.spark;

import com.ego.HadoopUtil;
import com.lucidworks.spark.util.ConfigurationConstants;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.lucidworks.spark.SparkApp;

import java.io.IOException;

public class SparkHiveSolrConvert {

    public static void main(String[] args) throws Exception {
        String sqlString = args[0];
        String collection = args[1];

        String zkhost = "hadoop-prod07:2181/solr";  // 好像只能写一个地址？

        SparkSession spark = HadoopUtil.createSparkSession("Spark Hive To Solr With DataFrame");
        Dataset<Row> df = spark.sql(sqlString);
        df.show();
        df.printSchema();

        df.write()
                .format("solr")
                .option("zkhost", zkhost)
                .option("collection", collection)
                .mode(SaveMode.Overwrite)
                .save();
        // 必须commit
        SolrSupport.getCachedCloudClient(zkhost).commit(collection);

        // 读取solr数据为dataframe
        Dataset<Row> dfDocs = spark.read()
                .format("solr")
                .option("zkhost", zkhost)
                .option("collection", "dim_date")
                // .option("query", "*:*")  // 默认"*:*" ConfigurationConstants.SOLR_QUERY_PARAM() = "query"
                // .option(ConfigurationConstants.SOLR_FIELD_PARAM(), "id,date_str,date_dt,year,yearmonth")  // ConfigurationConstants.SOLR_FIELD_PARAM() = "fields"
                // .option(ConfigurationConstants.SOLR_FILTERS_PARAM(), "dt") // ConfigurationConstants.SOLR_FILTERS_PARAM() = "filters"
                .load();
        dfDocs.show();
        dfDocs.printSchema();
        System.out.println("Default Total Rows: " + dfDocs.count());

        dfDocs.createOrReplaceTempView("solr_table");
        spark.sql("drop table if exists tmp.solr_tmp_test");
        spark.sql("create table tmp.solr_tmp_test stored as parquet as select t.*, 'solr' as data_source from solr_table t");

        // 必须添加，否则不会结束
        spark.stop();
    }
}
