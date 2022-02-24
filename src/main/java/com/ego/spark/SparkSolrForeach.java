package com.ego.spark;

import com.ego.HadoopUtil;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataType;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * solr如何创建collection（document）
 * 导入进solr的数据会显示带中括号[]，因为默认配置文件Config的 multiValued="true" 导致，默认是可以存储多个值，相当于list
 */

public class SparkSolrForeach {

    private static final String UNIQUE_KEY = "id";
    private static final String DEFAULT_FIELD_TYPE = "text_general";
    private static final Map<String, String> DATA_TYPE_MAP = new HashMap<>();

    static {
        // // pints plongs pfloats pdoubles pdates strings text_general(忽略大小写) // 后缀带s的multiValued="true"，所以可以直接使用不带后缀s的字段类型fieldType
        // DATA_TYPE_MAP.put("boolean", "boolean");
        // DATA_TYPE_MAP.put("byte", "");
        // DATA_TYPE_MAP.put("binary", "binary");
        DATA_TYPE_MAP.put("short", "pint");
        DATA_TYPE_MAP.put("integer", "pint");
        DATA_TYPE_MAP.put("long", "plong");
        DATA_TYPE_MAP.put("float", "pfloat");
        DATA_TYPE_MAP.put("double", "pdouble");
        DATA_TYPE_MAP.put("decimal", "pdouble");
        DATA_TYPE_MAP.put("string", "text_general");
        DATA_TYPE_MAP.put("date", "pdate");  // text_general
        DATA_TYPE_MAP.put("timestamp", "pdate");  // text_general
    }

    public static JsonArray getSchemaFields(String url, String data) throws Exception {
        // https://www.cnblogs.com/leeSmall/p/9103117.html Schema API 参考
        // http://10.63.82.192:8983/api/cores/tmp_test_shard1_replica_n1/schema/fields
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);

        CloseableHttpResponse response = client.execute(get);
        HttpEntity entity = response.getEntity();
        String responseContent = EntityUtils.toString(entity, "UTF-8");
        System.out.println("Get: " + responseContent);
        System.out.println("StatusCode: " + response.getStatusLine().getStatusCode());
        response.close();
        client.close();

        // String to JsonObject
        JsonObject responseJson = new JsonParser().parse(responseContent).getAsJsonObject();
        // json to map
        // Map responseMap = new Gson().fromJson(responseJson, Map.class);
        // Map/List to jsonString
        // new Gson().toJson(Map/List);

        String findKey = "name";
        if (data.equals("copyFields")) {
            findKey = "source";
        }

        // System.out.println(responseJson.getAsJsonArray(data));
        JsonArray jsonArray = new JsonArray();
        for (JsonElement element : responseJson.getAsJsonArray(data)) {
            JsonObject field = element.getAsJsonObject();
            if (!field.get(findKey).getAsString().startsWith("_") && !field.get(findKey).getAsString().equals(UNIQUE_KEY)) {  // 必须使用getAsString，如果使用toString返回的结果会带双引号
                JsonObject object = new JsonObject();
                object.add(findKey, field.get(findKey));
                if (findKey.equals("source")) {
                    object.add("dest", field.get("dest"));
                }
                jsonArray.add(object);
            }
        }
        return jsonArray;
    }

    public static void modifyFields(String url, JsonObject jsonData) throws Exception {
        /**
         * https://www.cnblogs.com/leeSmall/p/9103117.html Schema API 参考
         *
         * V1老版本的api，V2新版本的API，当前两个版本的API都支持，将来会统一到新版本。两个版本的API只是请求地址上的区别，参数没区别。
         * 一个使用collection，一个使用core，core_name类似具体文件需要增加_shard1_replica_n1之类的
         * V1： http://localhost:8983/solr/collection_name/schema
         * V2： http://localhost:8983/api/cores/core_name/schema
         * doc.addField("id", UUID.randomUUID().toString());
         *
         * 一次添加多个字段
         * {
         *     "add-field": [
         *         {
         *             "name": "shelf",
         *             "type": "myNewTxtField",
         *             "stored": true
         *         },
         *         {
         *             "name": "location",
         *             "type": "myNewTxtField",
         *             "stored": true
         *         }
         *     ]
         * }
         *
         */
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost post = new HttpPost(url);
        // post.addHeader("Content-type","application/json; charset=utf-8");
        // post.setHeader("Accept", "application/json");

        StringEntity myEntity = new StringEntity(jsonData.toString(), ContentType.APPLICATION_JSON);// 构造请求数据
        post.setEntity(myEntity);// 设置请求体

        CloseableHttpResponse response = client.execute(post);
        HttpEntity entity = response.getEntity();
        String responseContent = EntityUtils.toString(entity, "UTF-8");
        System.out.println("Post: " + responseContent);
        System.out.println("StatusCode: " + response.getStatusLine().getStatusCode());
        response.close();
        client.close();
    }

    public static void truncateCollection(List<String> solrUrls, String collection) throws IOException, SolrServerException {
        CloudSolrClient solrClient = new CloudSolrClient.Builder(solrUrls).build();
        solrClient.setDefaultCollection(collection);
        solrClient.deleteByQuery("*:*");
        solrClient.commit();
        solrClient.close();
        System.out.println("Truncated " + collection);
    }

    public static void main(String[] args) throws Exception {
        LocalDateTime startTime = LocalDateTime.now();
        String sqlString = args[0];
        String collection = args[1];
        int batchSize = Integer.parseInt(args[2]);
        int partitionNum = Integer.parseInt(args[3]);

        SparkSession spark = HadoopUtil.createSparkSession("Hive To Solr");
        Dataset<Row> df = spark.sql(sqlString);

        String solrUrl = "http://10.63.82.192:8983/solr";
        List<String> solrUrls = Collections.singletonList(solrUrl);

        // 删除copy fields
        JsonArray copyFieldsSchema = getSchemaFields(solrUrl + "/" + collection + "/schema/copyfields", "copyFields");
        JsonObject delCopyJsonData = new JsonObject();
        delCopyJsonData.add("delete-copy-field", copyFieldsSchema);
        System.out.println(delCopyJsonData);
        modifyFields(solrUrl + "/" + collection + "/schema", delCopyJsonData);

        // 删除fields
        JsonArray rawfieldsSchema = getSchemaFields(solrUrl + "/" + collection + "/schema/fields", "fields");
        JsonObject delJsonData = new JsonObject();
        delJsonData.add("delete-field", rawfieldsSchema);
        System.out.println(delJsonData);
        modifyFields(solrUrl + "/" + collection + "/schema", delJsonData);

        // pints plongs pfloats pdoubles pdates strings text_general(忽略大小写) // 后缀带s的multiValued="true"，所以可以直接使用不带后缀s的字段类型fieldType
        JsonObject col1 = new JsonObject();
        col1.addProperty("name", "aa1");
        col1.addProperty("type", "text_general");
        col1.addProperty("stored", true);   // stored即是否存储，如果为false则不会显示，查询也不会有该字段的内容
        col1.addProperty("indexed", true);  // indexed即建立索引，方便查询，或者当作where过滤字段等
        JsonObject col2 = new JsonObject();
        col2.addProperty("name", "aa2");
        col2.addProperty("type", "text_general");  // text_general
        // col2.addProperty("stored", "true");
        col2.addProperty("multiValued", "false");
        JsonObject col3 = new JsonObject();
        col3.addProperty("name", "aa3");
        col3.addProperty("type", "plongs");

        JsonArray fieldsJsonArray = new JsonArray();
        fieldsJsonArray.add(col1);
        fieldsJsonArray.add(col2);
        fieldsJsonArray.add(col3);

        List<String> columnsRaw = Arrays.asList(df.schema().fieldNames());
        // org.apache.spark.sql.functions 函数很多强无敌
        if (!columnsRaw.contains("id")) {
            // df 添加id列
            df = df.withColumn("id", functions.lit(UUID.randomUUID().toString()));  // uuid
            // df = df.withColumn("id", functions.monotonically_increasing_id());  // 自增id，由于是分布式会跨分区提前分配范围内的固定id且不重复，所以并不是真正连续的自增
        }
        if (!columnsRaw.contains("dt")) {
            // df 添加dt列
            df = df.withColumn("dt", functions.current_timestamp());
            // df = df.withColumn("dt", functions.lit(LocalDateTime.now().toString()));
        }
        // if (!columnsRaw.contains("t_t")) {
        //     // df 添加固定值列
        //     df = df.withColumn("t_t", functions.lit("#"))
        //             .withColumn("num", functions.lit(10));
        //     // 各列之间的加减乘除 plus(+) (-) multiply(*) divide(/) mod(取余数)  -- 没有的列无法使用链方式，必须分开写
        //     df = df.withColumn("num1", df.col("num").plus(1))
        //             .withColumn("num2", df.col("num").plus(-1))
        //             .withColumn("num3", df.col("num").multiply(10))
        //             .withColumn("num4", df.col("num").divide(5))
        //             .withColumn("num5", df.col("num").mod(3));
        //     // 计算公式很长的话推荐使用
        //     df = df.withColumn("num_expr", functions.expr("num * 100"));
        //     df = df.withColumn("null_col", functions.lit(null));
        //     df = df.withColumn("null_col_string", functions.lit(null).cast("string"));
        //     // 新增字段也推荐该方法
        //     df = df.selectExpr(
        //             "*",
        //             "cast(num as string) as num_string",
        //             "num1 + num2 - num3 * num4 / num as num_calc",
        //             "now() as dt_now"
        //     );
        // }
        df.printSchema();
        // df.show();
        System.out.println("getNumPartitions: " + df.toJavaRDD().getNumPartitions());
        System.out.println("partitionsSize: " + df.toJavaRDD().partitions().size());
        if (partitionNum != 0) {
            // Rdd分区过多的情况下，会由于过高频繁的插入造成SolrCloud崩溃
            // df = df.repartition(partitionNum);
            df = df.coalesce(partitionNum);
            System.out.println("getNumPartitions: " + df.toJavaRDD().getNumPartitions());
        }

        JsonArray addFieldsSchema = new JsonArray();
        for (StructField columnStruct : df.schema().fields()) {
            if (columnStruct.name().equals(UNIQUE_KEY)) {
                continue;
            }
            String name = columnStruct.name();
            // columnStruct.dataType().typeName()  返回值string, decimal(38,4)
            // columnStruct.dataType().toString()  返回值StringType, DecimalType(38,4)
            String dateType = columnStruct.dataType().typeName();

            // solr没有decimal类型，这里执行操作df转换，就不用在循环里面一个一个判断和转换
            if (dateType.startsWith("decimal")) {
                dateType = "decimal";
                df = df.withColumn(name, df.col(name).cast("double"));
            }
            JsonObject field = new JsonObject();
            field.addProperty("name", name);
            field.addProperty("type", DATA_TYPE_MAP.getOrDefault(dateType, DEFAULT_FIELD_TYPE));
            field.addProperty("multiValued", false);
            // stored 和 indexed 默认是true，所以可以不用设置
            // field.addProperty("stored", true);   // stored即是否存储，如果为false则不会显示，查询也不会有该字段的内容
            // field.addProperty("indexed", true);  // indexed即建立索引，方便查询，或者当作where过滤字段等
            addFieldsSchema.add(field);
        }
        JsonObject addJsonData = new JsonObject();
        addJsonData.add("add-field", addFieldsSchema);
        // addJsonData.add("add-field", fieldsJsonArray);
        System.out.println(addJsonData);

        // core 和 collection 还不太一样，core具体到数据文件？collection只是一个文档名称？
        // String core = collection + "_shard1_replica_n1";
        // addFields("http://10.63.82.192:8983/api/cores/" + core + "/schema", data);
        modifyFields(solrUrl + "/" + collection + "/schema", addJsonData);

        // 删除数据
        truncateCollection(solrUrls, collection);

        // 写入数据
        List<String> columns = Arrays.asList(df.schema().fieldNames());
        df.foreachPartition(iter -> {
            // CloudSolrClient solrClient = new CloudSolrClient.Builder(solrUrls).build();

            // 第一种方式：使用运行中的某一台solr节点
            // List<String> solrUrls = Collections.singletonList("http://10.63.82.192:8983/solr");
            // CloudSolrClient solrClient = new CloudSolrClient.Builder(solrUrls).build();

            // 第二种方式：使用zookeeper节点连接（推荐）Optional设置solr的根目录
            List<String> zkHost = Arrays.asList("hadoop-prod10:2181;hadoop-prod02:2181;hadoop-prod07:2181".split(";"));
            CloudSolrClient solrClient = new CloudSolrClient.Builder(zkHost, Optional.of("/solr")).build();

            solrClient.setDefaultCollection(collection);

            Collection<SolrInputDocument> docs = new ArrayList<>();
            for (int i = 0; i <= batchSize && iter.hasNext(); i++) {
                Row row = iter.next();
                SolrInputDocument doc = new SolrInputDocument();
                for (String columnName : columns) {
                    // 注意：如果value是null，web页面该字段是不会查询并显示出来的
                    doc.addField(columnName, row.getAs(columnName));
                    // System.out.println(columnName + ": " + row.getAs(columnName).toString());
                }
                docs.add(doc);
                if (i + 1 == batchSize) {
                    solrClient.add(docs);
                    solrClient.commit();
                    docs = new ArrayList<>();
                    i = -1;
                }
            }
            if (!docs.isEmpty()) {
                solrClient.add(docs);
                solrClient.commit();
            }
        });

        LocalDateTime endTime = LocalDateTime.now();
        Duration d = Duration.between(startTime, endTime);
        System.out.println("Total spent: " + d.toMillis() / 1000 + "s");

    }
}

