package com.ego.examples;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CaseSolr {

    public static void insert (CloudSolrClient solrClient) throws Exception {
        //创建索引文档对象
        SolrInputDocument doc = new SolrInputDocument();
        // 第一个参数：域的名称，域的名称必须是在schema.xml中定义的
        // 第二个参数：域的值,注意：id的域不能少
        doc.addField("id",0);
        doc.addField("name","红豆");
        doc.addField("price",1.2);
        //3.将文档写入索引库中
        solrClient.add(doc);
        solrClient.commit();

        // 批量写入
        SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField( "id", 1);
        doc1.addField( "name", "绿豆");
        doc1.addField( "price", 1.8 );
        doc1.addField( "price_double", 1.8 );
        doc1.addField( "zxcvasdf", 66.66 );
        SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField( "id", "2" );
        doc2.addField( "name", "黑豆" );
        doc2.addField( "price", 2.6 );
        SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField( "id", "3" );
        doc3.addField( "name", "huang豆" );
        doc3.addField( "price", 9.9 );
        SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField( "id", "4" );
        doc4.addField( "name", "黄豆" );
        doc4.addField( "price", 3.6 );
        Collection<SolrInputDocument> docs = new ArrayList<>();
        docs.add(doc1);
        docs.add(doc2);
        docs.add(doc3);
        docs.add(doc4);
        //3.将文档写入索引库中
        solrClient.add(docs);
        solrClient.commit();
    }

    public static void update (CloudSolrClient solrClient) throws Exception {
        SolrInputDocument doc4 = new SolrInputDocument();
        // 如下操作是对应id的整条记录的覆盖(重新添加)
        // doc4.addField( "id", "4" );
        // doc4.addField( "name_current", "我是新的黄豆的名字" );

        // 首先查询，然后基于查询结果进行修改
        // SolrQuery query = new SolrQuery("q","*:*");
        // SolrQuery query = new SolrQuery("q","id:4");
        // QueryResponse response = solrClient.query(query);
        // SolrDocumentList docs = response.getResults();
        // SolrDocument doc = docs.get(0);

        // SolrDocument to SolrInputDocument
        SolrDocument doc = solrClient.getById(String.valueOf(4));
        for (String name : doc.getFieldNames()) {
            doc4.addField(name, doc.getFieldValue(name));
        }
        // 然后再修改当前的SolrInputDocument
        doc4.setField("name", "我是新的黄豆的名字");
        doc4.setField("name_history", "黄豆");
        solrClient.add(doc4);
        solrClient.commit();
    }

    public static void delete (CloudSolrClient solrClient) throws Exception {
        //全删
        //solrClient.deleteByQuery("*:*");
        //模糊匹配删除（带有分词效果的删除），如果输入"huang豆"会先进行分词，然后把包含"豆"的doc也都删除掉。慎用。
        solrClient.deleteByQuery("name:huang");
        //指定id删除
        solrClient.deleteById("3");

        //通过id批量删除
        ArrayList<String> ids = new ArrayList<>();
        ids.add("2");
        ids.add("3");
        solrClient.deleteById(ids);

        solrClient.commit();
    }

    public static void query (CloudSolrClient solrClient) throws Exception {
        // 创建搜索对象
        SolrQuery query = new SolrQuery();
        // 设置搜索条件
        query.set("q","*:*");
        //设置每页显示多少条
        query.setRows(10);
        //发起搜索请求
        QueryResponse response = solrClient.query(query);
        // 查询结果
        SolrDocumentList docs = response.getResults();
        // 查询结果总数
        long cnt = docs.getNumFound();
        System.out.println("总条数为"+cnt+"条");
        for (SolrDocument doc : docs) {
            List<String> output = new ArrayList<>();
            for (String filed : doc.getFieldNames()){
                if (filed.equals("_version_")){ continue;}
                output.add(filed + ":" + doc.get(filed));
            }
            System.out.println(String.join(", ", output));
        }
    }

    public static void main(String[] args) throws Exception {
        // 第一种方式：使用运行中的某一台solr节点
        // List<String> solrUrls = Collections.singletonList("http://10.63.82.192:8983/solr");
        // CloudSolrClient solrClient = new CloudSolrClient.Builder(solrUrls).build();

        // 第二种方式：使用zookeeper节点连接（推荐）Optional设置solr的根目录
        List<String> zkHost = Arrays.asList("10.63.82.192:2181;10.63.82.199:2181;10.63.82.204:2181".split(";"));
        CloudSolrClient solrClient = new CloudSolrClient.Builder(zkHost, Optional.of("/solr")).build();

        System.out.println(solrClient);
        solrClient.setDefaultCollection("tmp_test");

        insert(solrClient);
        update(solrClient);
        delete(solrClient);
        query(solrClient);

        solrClient.close();
    }
}
