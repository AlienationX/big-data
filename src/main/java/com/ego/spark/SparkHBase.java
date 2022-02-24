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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ego.HadoopUtil;

public class SparkHBase {

    public final static String COLUMN_FAMILY = "cf";
    private static final Map<String, DataType> DATA_TYPE_MAP = new HashMap<>();

    static {
        DATA_TYPE_MAP.put("boolean", DataTypes.BooleanType);
        DATA_TYPE_MAP.put("byte", DataTypes.ByteType);
        DATA_TYPE_MAP.put("binary", DataTypes.BinaryType);
        DATA_TYPE_MAP.put("short", DataTypes.ShortType);
        DATA_TYPE_MAP.put("int", DataTypes.IntegerType);
        DATA_TYPE_MAP.put("integer", DataTypes.IntegerType);
        DATA_TYPE_MAP.put("long", DataTypes.LongType);
        DATA_TYPE_MAP.put("float", DataTypes.FloatType);
        DATA_TYPE_MAP.put("double", DataTypes.DoubleType);
        DATA_TYPE_MAP.put("decimal", DataTypes.DoubleType);  // 默认使用double
        // DATA_TYPE_MAP.put("decimal", DataTypes.createDecimalType(38, 3));  // 需要传入精度
        DATA_TYPE_MAP.put("string", DataTypes.StringType);
        DATA_TYPE_MAP.put("date", DataTypes.DateType);
        DATA_TYPE_MAP.put("timestamp", DataTypes.TimestampType);
    }

    public static Object getValueConvertDataType(String dataType, byte[] value) {
        if (value == null) {
            // 没有实际意义，因为hbase不存储null值
            return null;
        }

        Object o;
        if ("boolean".equals(dataType)) {
            o = Bytes.toBoolean(value);
        } else if ("byte".equals(dataType)) {
            o = value;  // 暂不支持的数据类型
        } else if ("binary".equals(dataType)) {
            o = Bytes.toStringBinary(value);  // 暂不支持的数据类型
        } else if ("short".equals(dataType)) {
            o = Bytes.toShort(value);
        } else if ("int".equals(dataType) || "integer".equals(dataType)) {
            // o = Bytes.toInt(value);  // 报错：offset (0) + length (4) exceed the capacity of the array: 2
            o = Integer.parseInt(Bytes.toString(value));
        } else if ("long".equals(dataType)) {
            // o = Bytes.toLong(value);
            o = Long.valueOf(Bytes.toString(value));
        } else if ("float".equals(dataType)) {
            // o = Bytes.toFloat(value);
            o = Float.valueOf(Bytes.toString(value));
        } else if ("double".equals(dataType)) {
            // o = Bytes.toDouble(value);
            o = Double.parseDouble(Bytes.toString(value));
        // } else if ("decimal".equals(dataType)) {
        } else if (dataType.startsWith("decimal")) {
            o = Bytes.toBigDecimal(value);
        } else if ("string".equals(dataType)) {
            o = Bytes.toString(value);
        } else if ("date".equals(dataType)) {
            o = Date.valueOf(Bytes.toString(value));
        } else if ("timestamp".equals(dataType)) {
            o = Timestamp.valueOf(Bytes.toString(value));
        } else {
            o = Bytes.toString(value);
        }
        return o;
    }

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

    public static List<String> getListColumns(Table table, String rowKey, String family) throws IOException {
        // 但是列名和rowKey绑定，每个rowKey的列名可能都不一样，需要加上rowKey获取。
        List<String> list = new ArrayList<>();
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            list.add(Bytes.toString(entry.getKey()));
            System.out.println("column " + Bytes.toString(entry.getKey()) + ", value " + Bytes.toString(entry.getValue()));
        }
        return list;
    }

    /**
     * @param df
     * @param hbaseConf
     * @throws IOException
     *
     * JavaPairRDD<ImmutableBytesWritable, Put> to HBase
     */
    public static void hiveToHBaseCommon(Dataset<Row> df, Configuration hbaseConf) throws IOException {
        Job job = Job.getInstance(hbaseConf);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "mr_detail");
        job.setOutputFormatClass(TableOutputFormat.class);

        // List<String> fields = new ArrayList<>();
        // df.printSchema();
        // System.out.println(Arrays.toString(df.schema().fields()));
        // for (StructField field : df.schema().fields()) {
        //     System.out.println(field.name());
        //     fields.add(field.name());
        // }

        List<String> fields = Arrays.asList(df.schema().fieldNames());

        df.toJavaRDD().mapToPair(row -> {
            // 参数可以增加int rowKeyIndex，设置rowkey字段。循环时rowkey字段跳过即可。
            // 联合主键同理，参数变成list，待完善
            Put put = new Put(Bytes.toBytes(row.get(0).toString()));
            for (int i = 1; i < fields.size(); i++) {
                if (row.get(i) != null) {
                    byte[] columnFamily = Bytes.toBytes(COLUMN_FAMILY);
                    byte[] qualifier = Bytes.toBytes(fields.get(i));
                    byte[] value = Bytes.toBytes(row.get(i).toString());
                    put.addColumn(columnFamily, qualifier, value);
                }
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        }).saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    /**
     * @param df
     * @param hbaseConf
     * @param tableName="mr_detail"
     * @param primaryKey="id,name"
     * @throws Exception
     */
    public static void hiveToHBaseWithHFileBulkLoad(Dataset<Row> df, Configuration hbaseConf, String tableName, String primaryKey) throws Exception {

        List<String> fields = Arrays.asList(df.schema().fieldNames());
        List<String> rowKeys = Arrays.asList(primaryKey.split("\\s*,\\s*"));
        List<String> dataFields = new ArrayList<>(fields);
        dataFields.removeAll(rowKeys);
        Collections.sort(dataFields);

        System.out.println(fields);
        System.out.println(rowKeys);
        System.out.println(dataFields);

        JavaPairRDD<String, Row> storedPairRDD = df.toJavaRDD().mapToPair(row -> {
            String rowKey = "";
            List<Object> data = new ArrayList();
            for (String field : fields) {
                if (rowKeys.contains(field)) {
                    rowKey += row.getAs(field).toString();
                } else {
                    data.add(row.getAs(field));
                }
            }
            System.out.println(data);
            System.out.println(data.toArray());
            return new Tuple2<>(rowKey, RowFactory.create(data.toArray()));  // 结果展开，否则传入的是一个list值。其实也可以直接使用
        }).sortByKey();  // rowkey排序，必须

        storedPairRDD.collect().forEach(System.out::println);

        // spark写hfile时候是按照rowkey+列族+列名进行排序的，因此在写入数据的时候，要做到整体有序，否则报错：Added a key not lexically larger than previous
        // 且必须要保证没有重复值，即相同的rowkey不能写入第二次
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileContent = storedPairRDD.flatMapToPair(x -> {
            List<Tuple2<ImmutableBytesWritable, KeyValue>> result = new ArrayList<>();
            byte[] rowKey = Bytes.toBytes(x._1);
            byte[] columnFamily = Bytes.toBytes(COLUMN_FAMILY);
            for (int i = 0; i < dataFields.size(); i++) {
                if (x._2.get(i) == null) {
                    continue;  // null值过滤，否则会报java.lang.NullPointerException
                }
                byte[] qualifier = Bytes.toBytes(dataFields.get(i));
                byte[] value = Bytes.toBytes(x._2.get(i).toString());
                KeyValue keyValue = new KeyValue(rowKey, columnFamily, qualifier, value);
                System.out.println("==> " + x._1 + "," + dataFields.get(i) + ":" + x._2.get(i).toString() + " " + keyValue);
                result.add(new Tuple2<>(new ImmutableBytesWritable(rowKey), keyValue));
            }
            return result.iterator();
        });

        Configuration config = new Configuration(hbaseConf);
        config.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
        config.set("mapreduce.job.output.key.class", "org.apache.hadoop.hbase.io.ImmutableBytesWritable");
        config.set("mapreduce.job.output.value.class", "org.apache.hadoop.hbase.KeyValue");
        config.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);

        // 生成HFile，路径会自动创建，如果存在会报错：org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory /tmp/hbase/bulkload/mr_detail_with_hfile already exists
        String hdfsOutputDir = "/tmp/hbase/bulkload/" + tableName + "_" + System.currentTimeMillis();
        hFileContent.saveAsNewAPIHadoopFile(hdfsOutputDir, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, config);

        // Bulk load HFile to HBase，路径文件会被迁移走，不是复制。路径文件的删除？
        Connection hbaseConnection = ConnectionFactory.createConnection(config);
        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = hbaseConnection.getRegionLocator(TableName.valueOf(tableName));
        Admin admin = hbaseConnection.getAdmin();
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
        try {
            loader.doBulkLoad(new Path(hdfsOutputDir), admin, table, regionLocator);
        } finally {
            hbaseConnection.close();
            table.close();
            admin.close();
            regionLocator.close();
        }
    }


    /**
     * @param spark
     * @param hbaseConf
     * @param tableName
     * @throws IOException
     *
     * HBase To JavaPairRDD<ImmutableBytesWritable, Result>
     */
    public static void hbaseToHBase(SparkSession spark, Configuration hbaseConf, String tableName) throws IOException {
        // 将HBase数据转成PairRDD
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

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
                System.out.println(rowKey + ":" + new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                        + ":" + new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                        + ":" + new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + ":" + new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                        + ":" + cell.getTimestamp()
                        + "(" + LocalDateTime.ofInstant(Instant.ofEpochMilli(cell.getTimestamp()), ZoneOffset.of("+8")) + ")");
                System.out.println(cell.getValueArray() + "," + cell.getValueOffset() + "," + cell.getValueLength());
                // put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
                put.add(cell);  // 推荐
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        }).saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    /**
     * @param spark
     * @param hbaseConf
     * @param tableName
     * @param struct="rowkey:id:string; cf1:name:string; cf1:name_cn:string; cf1:sex:string; cf1:age:int; cf2:name:string; cf2:amount:double; cf2:fee:decimal(38,2)"
     *
     * 注意事项：
     * 1. 必须写rowkey字段
     * 2. 支持decimal(38,3)类型
     * 3. 支持填写不存在的字段，会都赋值为null
     * 4. 少写的字段表结构不会创建少写的字段
     *
     * 字段类型使用spark的df.schema().fields().dataType().typeName()的小写类型
     * StructField columnStruct : df.schema().fields()
     * columnStruct.dataType().typeName()  返回值string, decimal(38,4)
     * columnStruct.dataType().toString()  返回值StringType, DecimalType(38,4)
     *
     * StructType schema = new StructType(new StructField[]{
     *     new StructField("frequent_set", DataTypes.StringType, false, Metadata.empty()),
     *     new StructField("k", DataTypes.IntegerType, false, Metadata.empty()),
     *     new StructField("num", DataTypes.IntegerType, false, Metadata.empty()),
     *     new StructField("row_num", DataTypes.LongType, false, Metadata.empty()),
     * });
     */
    public static void hbaseToHive(SparkSession spark, Configuration hbaseConf, String tableName, String struct) {

        // 表结构解析
        Map<String, DataType> hiveStructMap = new LinkedHashMap<>();  // 注意字段顺序
        for (String column : struct.split("\\s*;\\s*")) {
            String columnName = column.split(":")[0].trim() + "_" + column.split(":")[1].trim();
            String columnType = column.split(":")[2].trim();
            System.out.println(columnName + "," + columnType);
            DataType dataType = DATA_TYPE_MAP.getOrDefault(columnType, DataTypes.StringType);
            if (columnType.contains("decimal")){
                int precision = Integer.parseInt(columnType.substring(8, columnType.indexOf(",")));
                int scale = Integer.parseInt(columnType.substring(columnType.indexOf(",") + 1, columnType.indexOf(")")));
                dataType = DataTypes.createDecimalType(precision, scale);
            }
            hiveStructMap.put(columnName, dataType);
        }
        System.out.println("Hive Struct Map: " + hiveStructMap);

        Map<String, List<String>> hbaseStructMap = new HashMap<>();

        List<StructField> structFields = new ArrayList<>();
        for (String key : hiveStructMap.keySet()) {
            structFields.add(new StructField(key , hiveStructMap.get(key), true, Metadata.empty()));
            // structFields.add(DataTypes.createStructField(key, structMap.get(key), true));

            if (key.contains("rowkey")) {
                continue;  // rowkey不需要存储
            }
            String columnFamily = key.split("_")[0];
            String qualifier = key.substring(key.indexOf("_") + 1);
            if (!hbaseStructMap.containsKey(columnFamily)) {
                hbaseStructMap.put(columnFamily, new ArrayList<>());
            }
            hbaseStructMap.get(columnFamily).add(qualifier);
        }
        System.out.println("HBase Struct Map: " + hbaseStructMap);
        // StructType -> List<StructField> -> StructField
        StructType schema = DataTypes.createStructType(structFields);
        System.out.println("DataFrame Struct Type: " + Arrays.toString(schema.fields()));

        // 将HBase数据转成PairRDD
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
        JavaPairRDD<ImmutableBytesWritable, Result>  hbasePairRDD= jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        hbasePairRDD.collect().forEach(System.out::println);

        // 不同列簇可以存在相同列名
        // 关于字段动态解析，因为每行的rowkey对应的列簇和列名可能不同，最后生成的DataFrame的StructField只有都读取完才能生成，且需要第二次遍历且列名没有值的需要设置成null，按照顺序生成Row数据。且字段类型无法判断设置，只能都设置成string
        // 基于以上原因，设置成固定列的参数传入并处理更加方便和字段类型准确
        JavaRDD<Row> hbaseRDD = hbasePairRDD.map(x -> {
            List<Object> data = new ArrayList<>();
            Result row = x._2;
            String rowkey = Bytes.toString(row.getRow());
            data.add(rowkey);
            for (String columnFamily: hbaseStructMap.keySet()) {
                for (String qualifier: hbaseStructMap.get(columnFamily)){
                    // Object value = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier)));
                    // byte[] value = row.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                    byte[] value = row.getValue(columnFamily.getBytes(), qualifier.getBytes());
                    String dateType = hiveStructMap.get(columnFamily + "_" + qualifier).typeName();
                    System.out.println(rowkey + ":" +columnFamily + ":"+ qualifier + "=" + dateType + ", " + Bytes.toString(value) + ", " + value);
                    data.add(getValueConvertDataType(dateType, value));
                }
            }
            System.out.println(data);
            return RowFactory.create(data.toArray());
        }).cache();

        Dataset<Row> df = spark.createDataFrame(hbaseRDD, schema);
        df.show();
        df.printSchema();

        df.createOrReplaceTempView("hbase_table");
        spark.sql("drop table if exists tmp.hbase_table");
        spark.sql("create table tmp.hbase_table stored as parquet as select t.*, now() as etl_time from hbase_table t");
    }

    public static void main(String[] args) throws Exception {

        String tableName = "Contacts";

        SparkSession spark = HadoopUtil.createSparkSession("Spark HBase");
        spark.sparkContext().getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop-prod03:2181,hadoop-prod04:2181,hadoop-prod08:2181");
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
        // hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName + "_bak");

        // hbaseConf -> connection -> getAdmin(admin)
        // hbaseConf -> connection -> getTable(table)
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = conn.getAdmin();

        // Dataset<Row> df = spark.sql("select * from tmp.mr_detail t");
        Dataset<Row> df = spark.sql("select * from tmp.test1 t");
        df.printSchema();
        System.out.println(df.schema().fields()[0].metadata());

        for (StructField columnStruct : df.schema().fields()){
            System.out.println(columnStruct.dataType().typeName());
            System.out.println(columnStruct.dataType().toString());
        }

        createTable(admin, "mr_detail");
        hiveToHBaseCommon(df, hbaseConf);

        createTable(admin, "mr_detail_with_hfile");
        hiveToHBaseWithHFileBulkLoad(df, hbaseConf, "mr_detail_with_hfile", "id, charge_time");

        hbaseToHBase(spark, hbaseConf, tableName);
        hbaseToHive(spark, hbaseConf, "lshu_test:test_tb", "rowkey:id:string; f1:name:string; f1:name_en:string; f1:sex:string; f2:english:int; f2:math:double; f2:chinese:decimal(38,3)");

        Table table = conn.getTable(TableName.valueOf(tableName));
        List<String> families = getListColumnFamilies(table);
        System.out.println(families);
        List<String> columns = getListColumns(table, "1000", "Personal");
        System.out.println(columns);

        spark.stop();
    }
}
