package com.ego.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ego.HadoopUtil;


public class SparkHBase {

    public final static String COLUMN_FAMILY = "cf";

    public static Map getHBaseSchema() {
        // tableName:
        //     fimaly1: qualifier11,qualifier12,qualifier13,qualifier14... ...
        //     fimaly2: qualifier21,qualifier22,qualifier23,qualifier24... ...
        //     ... ...

        // Yaml yaml = new Yaml();
        // //文件路径是相对类目录(src/main/java)的相对路径
        // InputStream in = App.class.getClassLoader().getResourceAsStream("hbase_user_info.yaml");//或者app.yaml
        // Map<String, Object> map = yaml.loadAs(in, Map.class);

        Map<String, Object> map = new HashMap<>();
        // map.put("")

        return map;
    }

    public static void createTable(Admin admin, String tableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " already exists.");
        } else {
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
            hbaseTable.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
            admin.createTable(hbaseTable);
            System.out.println(tableName + " has been created.");
        }
    }

    public static List<String> getListColumnFamilies(Table table) throws IOException {
        List<String> list = new ArrayList<>();
        HTableDescriptor hTableDescriptor = table.getTableDescriptor();
        for (HColumnDescriptor family : hTableDescriptor.getColumnFamilies()) {
            list.add(family.getNameAsString());
        }
        return list;
    }

    public static List<String> getListColumns(Table table, String rowkey, String family) throws IOException {
        // 但是列名和rowkey绑定，每个rowkey的列名可能都不一样，需要加上rowkey获取。
        List<String> list = new ArrayList<>();
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            list.add(Bytes.toString(entry.getKey()));
            System.out.println("column " + Bytes.toString(entry.getKey()) + ", value " + Bytes.toString(entry.getValue()));
        }
        return list;
    }

    public static void hiveToHBaseCommon(Dataset<Row> df, Configuration hbaseConf) throws IOException {
        Job job = Job.getInstance(hbaseConf);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "mr_detail");
        job.setOutputFormatClass(TableOutputFormat.class);

        List<String> fields = new ArrayList<>();
        df.printSchema();
        System.out.println(Arrays.toString(df.schema().fields()));
        for (StructField field : df.schema().fields()) {
            System.out.println(field.name());
            fields.add(field.name());
        }

        df.toJavaRDD().mapToPair(row -> {
            // 参数可以增加int rowKeyIndex，设置rowkey字段。循环时rowkey字段跳过即可。
            // 联合主键同理，参数变成list，待完善
            Put put = new Put(Bytes.toBytes(row.get(0).toString()));
            for (int i = 1; i < fields.size(); i++) {
                if (row.get(i) != null) {
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(fields.get(i)), Bytes.toBytes(row.get(i).toString()));
                }
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        }).saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    public static void hiveToHBaseWithTable(Dataset<Row> df, Configuration hbaseConf) throws IOException {
        // Job job = Job.getInstance(hbaseConf);
        // job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "mr_detail_with_table");
        // job.setOutputFormatClass(TableOutputFormat.class);

        df.printSchema();
        System.out.println(Arrays.toString(df.schema().fields()));

        List<String> fields = new ArrayList<>();
        for (StructField field : df.schema().fields()) {
            System.out.println(field.name());
            fields.add(field.name());
        }

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD = df.toJavaRDD().mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.get(0).toString()));
            for (int i = 1; i < fields.size(); i++) {
                if (row.get(i) != null) {
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(fields.get(i)), Bytes.toBytes(row.get(i).toString()));
                }
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        // JavaRDD<Put> hbaseRDD = df.toJavaRDD().map(row -> {
        //     Put put = new Put(Bytes.toBytes(row.get(0).toString()));
        //     for (int i = 1; i < fields.size(); i++) {
        //         if (row.get(i) != null) {
        //             put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(fields.get(i)), Bytes.toBytes(row.get(i).toString()));
        //         }
        //     }
        //     return put;
        // });

        // error, 为解决
        // hbasePairRDD.foreachPartition(iter -> {
        //     Connection conn = ConnectionFactory.createConnection(hbaseConf);
        //     // HTable table= new HTable(job.getConfiguration(), );  // HTable过时的，推荐使用新版api的Table代替
        //     Table table = conn.getTable(TableName.valueOf("mr_detail_with_table"));
        //     while (iter.hasNext()) {
        //         table.put(iter.next()._2);
        //     }
        //     // table.put(iter.); // put(List)
        //     table.close();
        //     conn.close();
        // });
    }

    public static void hiveToHBaseWithHFile(Dataset<Row> df, Configuration hbaseConf, String tableName) throws Exception {
        // error 未成功 org.apache.hadoop.security.AccessControlException: Permission denied: user=work, access=WRITE, inode="/user":hdfs:supergroup:drwxr-xr-x

        // hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
        // hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        // Job job = Job.getInstance();
        // job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        // job.setMapOutputValueClass(KeyValue.class);
        // job.setOutputFormatClass(HFileOutputFormat2.class);
        //
        // Connection conn = ConnectionFactory.createConnection(hbaseConf);
        // // HRegionLocator regionLocator = new HRegionLocator(TableName.valueOf(tableName), (ClusterConnection) conn);  // HRegionLocator 和 HTable 过时的
        // RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
        // Table realTable = conn.getTable(TableName.valueOf(tableName));
        // HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
        //
        // List<String> fields = new ArrayList<>();
        // for (StructField field : df.schema().fields()) {
        //     System.out.println(field.name());
        //     fields.add(field.name());
        // }
        //
        // // 重点rowkey的排序，列族 + 列名 (字段顺序)的排序
        // // 调用 saveAsNewAPIHadoopFile 方法抛出 "Added a key not lexically larger than previous" 的异常是因为排序问题导致
        // JavaPairRDD<ImmutableBytesWritable, KeyValue> hbaseData = df.javaRDD().flatMapToPair(row -> {
        //     List<Tuple2<ImmutableBytesWritable, KeyValue>> kvs = new ArrayList<>();
        //
        //     String rowKey = Bytes.toString(row.getAs(0));
        //     ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
        //     for (int i = 1; i < fields.size(); i++) {
        //         // 控制 列族 + 列名 的排序
        //         kvs.add(new Tuple2<>(writable, new KeyValue(
        //                 Bytes.toBytes(rowKey),
        //                 Bytes.toBytes(COLUMN_FAMILY),  // COLUMN_FAMILY.getBytes(),
        //                 Bytes.toBytes(fields.get(i)),  // fields.get(i).getBytes(),
        //                 row.getAs(fields.get(i))
        //         )));
        //     }
        //     return kvs.iterator();
        // }).sortByKey(); // 这里让Spark去控制行键rowkey的排序
        //
        // // 创建HDFS的临时HFile文件目录
        // String hdfsOutputDir = "/tmp/hbase/bulkload/" + tableName + "_" + System.currentTimeMillis();
        // hbaseData.saveAsNewAPIHadoopFile(
        //         hdfsOutputDir,
        //         ImmutableBytesWritable.class,
        //         KeyValue.class,
        //         HFileOutputFormat2.class,
        //         job.getConfiguration()
        // );
        //
        // // Bulk load HFile to HBase
        // LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
        // Admin admin = conn.getAdmin();
        // loader.doBulkLoad(new Path(hdfsOutputDir), admin, realTable, regionLocator);
    }

    public static void hbaseToHbase(SparkSession spark, Configuration hbaseConf, String tableName) throws IOException {
        // 将HBase数据转成PairRDD
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(
                hbaseConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        hbaseRdd.collect().forEach(System.out::println);
        hbaseRdd.foreach(x -> {
            Result row = x._2;
            String rowKey = Bytes.toString(row.getRow());
            String c1 = Bytes.toString(row.getValue("Personal".getBytes(), "Name".getBytes()));
            String c2 = Bytes.toString(row.getValue("Personal".getBytes(), "Phone".getBytes()));
            String c3 = Bytes.toString(row.getValue("Office".getBytes(), "Address".getBytes()));
            String c4 = Bytes.toString(row.getValue("Office".getBytes(), "Phone".getBytes()));
            System.out.println(String.format("rowkey: %s, Personal:Name=%s, Personal:Phone=%s, Office:Address=%s, Office:Phone=%s", rowKey, c1, c2, c3, c4));
            for (Cell cell : x._2.listCells()) {
                System.out.println("rowkey=" + rowKey + ", cell=" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        });

        Job job = Job.getInstance(hbaseConf);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName + "_bak");
        job.setOutputFormatClass(TableOutputFormat.class);

        hbaseRdd.mapToPair(x -> {
            Put put = new Put(x._2.getRow());
            String rowKey = Bytes.toString(x._2.getRow());
            for (Cell cell : x._2.listCells()) {
                System.out.println(rowKey + ":" + Bytes.toString(cell.getRow()) + ":" + Bytes.toString(cell.getFamily()) + ":" + Bytes.toString(cell.getQualifier()) + ":" + Bytes.toString(cell.getValue()));
                System.out.println(rowKey + ":" + Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(rowKey
                        + ":" + new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                        + ":" + new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                        + ":" + new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + ":" + new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                        + ":" + cell.getTimestamp() + "(" + LocalDateTime.ofInstant(Instant.ofEpochMilli(cell.getTimestamp()), ZoneOffset.of("+8")) + ")"
                );
                System.out.println(cell.getValueArray() + "," + cell.getValueOffset() + "," + cell.getValueLength());
                // put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
                put.add(cell);  // 推荐
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        }).saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    public static void hbaseToHive() {
        // JavaRDD<Row> personsRDD = myRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>,Row>() {
        //     @Override
        //     public Row call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
        //         // TODO Auto-generated method stub
        //         System.out.println("====tuple=========="+tuple);
        //         Result result = tuple._2();
        //         String rowkey = Bytes.toString(result.getRow());
        //         List<String> list=new ArrayList<String>();
        //         list.add(rowkey);
        //         for (Map.Entry<String,String> entry:cfq.entrySet()){
        //             String cf=entry.getValue();
        //             String col=entry.getKey();
        //             String s=Bytes.toString(result.getValue(Bytes.toBytes(cf),Bytes.toBytes(col)));
        //             list.add(s);
        //         }
        //         //将tuple数据转换为rowRDD
        //         String[] fields =list.toArray(new String[list.size()]);
        //         return RowFactory.create(fields);
        //     }
        // });
        // List<StructField> structFields=new ArrayList<StructField>();
        // structFields.add(DataTypes.createStructField("rowkey", DataTypes.StringType, true));
        // List<String> fields=new ArrayList<String>(cfq.keySet());
        // for (int i=0;i<fields.size();i++){
        //     structFields.add(DataTypes.createStructField(fields.get(i),DataTypes.StringType,true));
        // }
        // //将hbase表映射为schema
        // StructType schema=DataTypes.createStructType(structFields);
        // Dataset stuDf=spark.createDataFrame(personsRDD, schema);
        // //注册表
        // stuDf.createOrReplaceTempView("c1");
        // Dataset<Row> nameDf=spark.sql("select * from c1");
        // nameDf.show();
    }

    public static void main(String[] args) throws Exception {

        String tableName = "Contacts";

        SparkSession spark = HadoopUtil.createSparkSession("Spark HBase");
        spark.sparkContext().getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop-prod03:2181,hadoop-prod04:2181,hadoop-prod08:2181");
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
        // hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName + "_bak");

        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = conn.getAdmin();

        Dataset<Row> df = spark.sql("select * from tmp.mr_detail t");
        createTable(admin, "mr_detail");
        hiveToHBaseCommon(df, hbaseConf);
        createTable(admin, "mr_detail_with_table");
        // hiveToHBaseWithTable(df, hbaseConf);
        createTable(admin, "mr_detail_with_hfile");
        hiveToHBaseWithHFile(df, hbaseConf, "mr_detail_with_hfile");

        Table table = conn.getTable(TableName.valueOf(tableName));
        List<String> families = getListColumnFamilies(table);
        System.out.println(families);
        List<String> columns = getListColumns(table, "1000", "Personal");
        System.out.println(columns);

        hbaseToHbase(spark, hbaseConf, tableName);

        spark.stop();
    }
}
