#!/usr/bin/env bash

# Elasticsearch 7.6.2 CRUD

# Create document
curl -H "Content-Type: application/json" -XPUT http://localhost:9200/samples/candidate/1?pretty -d '{
    "firstName": "Emerson",
    "lastName": "Atkins",
    "skills": ["Java", "Hadoop", "Elasticsearch"]
}'


# Retrieve(Read) document
curl -XGET http://localhost:9200/samples/candidate/1?pretty
curl -XGET http://localhost:9200/samples/candidate/1



# Update document，doc是关键字。如果被更新的文档不存在，可以在JSON的upsert部分中添加一个初始文档用于索引。
curl -H "Content-Type: application/json" -XPOST http://localhost:9200/samples/candidate/1/_update?pretty -d '{
    "doc": {
        "experience": 8
    }
}'

curl -H "Content-Type: application/json" -XPOST http://localhost:9200/samples/candidate/2/_update?pretty -d '{
    "doc": {
        "experience": 8
    },
    "upsert": {
        "firstName": "shu",
        "lastName": "Roger",
        "skills": ["Python", "Hadoop", "Elasticsearch", "Spark", "MapReduce"]
    }
}'


# Delete document
curl -XDELETE http://localhost:9200/samples/candidate/1?pretty


# Create index，没什么用，创建文档会默认创建index和type
curl -XPUT http://localhost:9200/medical?pretty





# 7版本之后彻底移除了映射类型mapping type，详情参考https://blog.51cto.com/14298057/2384062
# 7版本以后查询映射不能指定类型，只能查看整个索引的映射类型
curl -XPOST http://localhost:9200/samples/_mapping?pretty
# 7版本之前查询映射的方式，需要指定类型
curl -XPOST http://localhost:9200/samples/candidate/_mapping?pretty

# 创建映射类型