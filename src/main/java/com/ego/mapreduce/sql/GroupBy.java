package com.ego.mapreduce.sql;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.ego.HadoopUtil;

/**
 * create table tmp.mr_group stored as parquet as
 * select 'roger' as name,'2020-01-01' as dt,'milk' as item,5 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'milk' as item,5 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'bread' as item,10 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'orange' as item,3 as amount union all
 * select 'mike' as name,'2020-01-01' as dt,'bread' as item,12.5 as amount union all
 * select 'mike' as name,'2020-01-02' as dt,'orange' as item,6 as amount union all
 * select 'mike' as name,'2020-01-03' as dt,'orange' as item,6 as amount;
 *
 * create table tmp.mr_group_result stored as parquet as
 * select name,
 *        count(distinct item) as item_num,
 *        sum(amount) as sum_amount,
 *        avg(amount) as avg_amount,
 *        max(amount) as max_amount
 * from tmp.mr_group
 * group by name;
 *
 * yarn没有hive-hcatalog-core-1.1.0-cdh5.16.2.jar，所以需要job.addFileToClassPath单独添加
 * /opt/cloudera/parcels/CDH/lib/hive/lib下没有
 * /opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog存在
 */

public class GroupBy {
    private static Logger LOG = Logger.getLogger(GroupBy.class);

    public static class GroupMap extends Mapper<WritableComparable, HCatRecord, Text, IntWritable> {
        private HCatSchema fromSchema;

        @Override
        public void setup(Context context) throws IOException {
            fromSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws InterruptedException, IOException {
            String name = value.get("name", fromSchema).toString();

            // record.setString("item", inputSchema, value.get("item", inputSchema).toString());
            // record.setDouble("amount", inputSchema, (double) value.get("amount", inputSchema));
            // HCatRecord record = new DefaultHCatRecord(2);
            // record.set(0, value.get("item", fromSchema));
            // record.set(1, value.get("amount", fromSchema));

            // context.write(new Text(name), record);
            context.write(new Text(name),new IntWritable(1));
            // LOG.info(record.toString());
        }

    }

    public static class GroupReduce extends Reducer<Text, HCatRecord, WritableComparable, HCatRecord> {
        private HCatSchema toSchema;

        @Override
        public void setup(Context context) throws IOException {
            toSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void reduce(Text key, Iterable<HCatRecord> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            Set<String> items = new HashSet<>();
            double sumAmount = 0;
            double maxAmount = 0;

            for (HCatRecord value : values) {
                String item = value.get(0).toString();
                double amount = (double) value.get(1);

                cnt++;
                items.add(item);
                sumAmount += amount;
                if (amount > maxAmount) {
                    maxAmount = amount;
                }
            }
            // HCatRecord record = new DefaultHCatRecord();
            // record.set("name", schema,);
            HCatRecord record = new DefaultHCatRecord(5);
            record.set("name", toSchema, key.toString());
            record.set("item_num", toSchema, items.size());
            record.set("sum_amount", toSchema, sumAmount);
            record.set("avg_amount", toSchema, sumAmount / cnt);
            record.set("max_amount", toSchema, maxAmount);
            // record.set(0, key.toString());
            // record.set(1, itemNum);
            // record.set(2, sumAmount);
            // record.set(3, sumAmount/cnt);
            // record.set(4, minAmount);
            context.write(null, record);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getConf();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Group By <fromTable> <toTable>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "UseHCatGroupBy");
        job.setNumReduceTasks(1);
        HadoopUtil.addTmpJars(job);
        System.out.println(conf.get("yarn.application.classpath"));

        if (HadoopUtil.isDevelopment()) {
            job.setJar(HadoopUtil.LOCAL_JAR_NAME);
        } else {
            job.setJarByClass(GroupBy.class);
        }

        job.setMapperClass(GroupBy.GroupMap.class);
        job.setReducerClass(GroupBy.GroupReduce.class);

        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(DefaultHCatRecord.class);

        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        // job.setOutputValueClass(HCatRecord.class);

        HCatInputFormat.setInput(job, "tmp", "mr_group");
        job.setInputFormatClass(HCatInputFormat.class);

        HCatOutputFormat.setOutput(job, OutputJobInfo.create("tmp", "mr_group_result", null));
        job.setOutputFormatClass(HCatOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
