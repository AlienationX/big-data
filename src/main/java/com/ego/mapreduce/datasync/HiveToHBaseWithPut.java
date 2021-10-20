package com.ego.mapreduce.datasync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

import com.ego.HadoopUtil;

public class HiveToHBaseWithPut {

    private static Logger LOG = Logger.getLogger(HiveToHBaseWithPut.class);

    public static class PutMapper extends Mapper<WritableComparable, HCatRecord, ImmutableBytesWritable, Put> {

        private HCatSchema fromSchema;

        @Override
        public void setup(Context context) throws IOException {
            fromSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {

            List<String> fields = fromSchema.getFieldNames();
            String keyString = value.get(fields.get(0), fromSchema).toString();

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(keyString.getBytes());
            Put put = new Put(Bytes.toBytes(keyString));
            for (int i = 1; i < fields.size(); i++) {
                // 过滤字段的null值，hbase不存储null值。否则会报错Initialization of all the collectors failed. Error in last collector was:java.lang.NullPointerException
                Object o = value.get(fields.get(i), fromSchema);
                if (o != null) {
                    put.addColumn("cf".getBytes(), fields.get(i).getBytes(), o.toString().getBytes());
                }
            }

            context.write(rowKey, put);
            LOG.info(rowKey.toString() + ": " + put.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        // 权限问题未解决
        // Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied: user=hbase, access=WRITE, inode="/user":hdfs:supergroup:drwxr-xr-x

        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        Configuration hbaseConf = HBaseConfiguration.create();

        String[] otherArgs = new GenericOptionsParser(hbaseConf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Hive To HBase With Put <fromTable> <toTable>");
            System.exit(2);
        }
        // String fromTable = otherArgs[0];
        // String toTable = otherArgs[1];
        String fromTable = "tmp.mr_detail";
        String hbaseTable = "mr_detail_with_mapreduce_put";

        String p = "\\.";
        String fromDbName = fromTable.split(p)[0];
        String fromTbName = fromTable.split(p)[1];

        hbaseConf.set("hbase.zookeeper.quorum", "hadoop-prod03:2181,hadoop-prod04:2181,hadoop-prod08:2181");
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable);
        // hbaseConf.set("hbase.mapred.outputtable", hbaseTable);

        hbaseConf.set("mapreduce.app-submission.cross-platform", "true");
        hbaseConf.set("yarn.application.classpath", "$HADOOP_CLIENT_CONF_DIR"
                + ",$HADOOP_CONF_DIR"
                + ",$HADOOP_COMMON_HOME/*"
                + ",$HADOOP_COMMON_HOME/lib/*"
                + ",$HADOOP_HDFS_HOME/*"
                + ",$HADOOP_HDFS_HOME/lib/*"
                + ",$HADOOP_YARN_HOME/*"
                + ",$HADOOP_YARN_HOME/lib/*"
                + ",/opt/cloudera/parcels/CDH/lib/hive/lib/*"
                + ",/opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog/*");

        Job job = Job.getInstance(hbaseConf, "HiveToHBaseWithPut");
        job.setNumReduceTasks(1);

        if (HadoopUtil.isDevelopment()) {
            job.setJar(HadoopUtil.LOCAL_JAR_NAME);
        } else {
            job.setJarByClass(HiveToHBaseWithPut.class);
        }

        job.setMapperClass(PutMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // HCatInputFormat.setInput(job, "tmp", "mr_group");
        HCatInputFormat.setInput(job, fromDbName, fromTbName);
        job.setInputFormatClass(HCatInputFormat.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
