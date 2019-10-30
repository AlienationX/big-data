package com.ego.examples;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

// import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class CaseHBase {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop-dev01:2181,hadoop-dev02:2181,hadoop-dev03:2181");
        Connection conn;
        try {
            conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();

            // 查看所有表名
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                System.out.println(tableName.toString());
            }
            System.out.println("---------------------");

            // 创建命名空间
            NamespaceDescriptor ns = NamespaceDescriptor.create("lshu_test").build();
            NamespaceDescriptor[] nsd = admin.listNamespaceDescriptors();
            Boolean nsExists = false;
            for (NamespaceDescriptor n : nsd) {
                System.out.println("namespace: " + n.getName());
                if (ns.getName().equals(n.getName())) {
                    nsExists = true;
                }
            }
            if (!nsExists) {
                admin.createNamespace(ns);
                System.out.println("namespace" + ns.toString() + ns.getName() + "created");
            }

            // 创建表
            String tableName = "lshu_test:test_tb";
            Table table = conn.getTable(TableName.valueOf(tableName));
            if (admin.tableExists(table.getName())) {
                System.out.println(tableName + "is exists");
            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                tableDescriptor.addFamily(new HColumnDescriptor("f1"));
                tableDescriptor.addFamily(new HColumnDescriptor("f2"));
                admin.createTable(tableDescriptor);
                System.out.println(tableName + "created");
            }


            // 插入数据
            Put putOne = new Put(Bytes.toBytes("001"));  // 设置rowkey
            putOne.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("小明"));
            putOne.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name_en"), Bytes.toBytes("xiaoming"));
            putOne.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
            putOne.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("english"), Bytes.toBytes("88"));
            putOne.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("math"), Bytes.toBytes(String.valueOf(76)));  // 不能传入数字，否则会乱码，尽量将数字都转化成字符串，包括rowkey
            table.put(putOne);

            // 循环插入数据
            ArrayList<Put> insPuts = new ArrayList();
            for (int i = 1; i < 10; i++) {
                Put putI = new Put(Bytes.toBytes(String.valueOf(i)));
                putI.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("my is no" + i+"."));
                insPuts.add(putI);
            }
            table.put(insPuts);

            // 删除数据
            ArrayList<Delete> delPuts = new ArrayList<>();
            Delete delOne = new Delete(Bytes.toBytes(String.valueOf(6)));
            table.delete(delOne);

            for (int i = 8; i < 10; i++) {
                Delete del = new Delete(Bytes.toBytes(String.valueOf(i)));
                delPuts.add(del);
            }
            table.delete(delPuts);

            // 获取指定rowkey的表数据
            System.out.println("---------------------");
            String rowKey = "001";
            Get getOne = new Get(Bytes.toBytes(rowKey));
            Result resultOne = table.get(getOne);
            for (Cell cell : resultOne.listCells()) {
                System.out.println("rowkey:" + rowKey +
                        " family:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        " column:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        " value:" + Bytes.toString(CellUtil.cloneValue(cell)) +
                        " timestamp:" + cell.getTimestamp());
            }

            // 获取表全部数据
            // 扫描器
            System.out.println("---------------------");
            ResultScanner resultScanner = table.getScanner(new Scan());
            Iterator<Result> results = resultScanner.iterator();
            while (results.hasNext()) {
                Result result = results.next();
                for (Cell cell : result.listCells()) {
                    System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                            " family:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                            " column:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                            " value:" + Bytes.toString(CellUtil.cloneValue(cell)) +
                            " timestamp:" + cell.getTimestamp());
                }
            }

            // 删除表
            // TableName delTableName = TableName.valueOf(tableName);
            // admin.disableTable(delTableName);
            // admin.deleteTable(delTableName);
            // System.out.println(tableName + "deleted");


            // 删除命名空间
            // admin.deleteNamespace(ns.getName());
            // System.out.println("namespace" + ns.getName() + "deleted");

            table.close();
            admin.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
