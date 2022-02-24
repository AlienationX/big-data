package com.ego.spark;

/**
 * 导入到Solr有时间会因为并行度太大导致Solr服务挂掉，可能是部署问题，没有实现高并发高可用
 * 另一种方式是通过spark读取hive数据生成json文件到hdfs上，最后使用curl命令通过api导入到Solr
 *
 * curl http://localhost:8983/solr/baikeperson/update/json?commit=true --data-binary @/home/XXX/下载/person/test1.json -H 'Content-type:text/json; charset=utf-8'
 *
 * 生成的json格式参考：
 * {
 * 　　"add": {
 * 　　　　"overwrite": true,
 * 　　　　"doc": {
 * 　　　　　　"id": 1,
 * 　　　　　　"name": "Some book",
 * 　　　　　　"author": ["John", "Marry"]
 * 　　　　}
 * 　　},
 * 　　"add": {
 * 　　　　"overwrite": true,
 * 　　　　"boost": 2.5,
 * 　　　　"doc": {
 * 　　　　　　"id": 2,
 * 　　　　　　"name": "Important Book",
 * 　　　　　　"author": ["Harry", "Jane"]
 * 　　　　}
 * 　　},
 * 　　"add": {
 * 　　　　"overwrite": true,
 * 　　　　"doc": {
 * 　　　　　　"id": 3,
 * 　　　　　　"name": "Some other book",
 * 　　　　　　"author": "Marry"
 * 　　　　}
 * 　　}
 * }
 */
public class SparkSolrWithJson {
}
