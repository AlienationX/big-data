package com.ego.mapreduce.sql;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Aggregation {
    private static Logger LOG = Logger.getLogger(Aggregation.class);

    public static class AggregationMap extends Mapper<WritableComparable, HCatRecord, Text, HCatRecord> {
        private HCatSchema fromSchema;

        @Override
        public void setup(Context context) throws IOException {
            fromSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws InterruptedException, IOException {
            String name = value.get("name", fromSchema).toString();

            HCatRecord record = new DefaultHCatRecord(2);
            // record.setString("item", fromSchema, value.get("item", fromSchema).toString());
            // record.setDouble("amount", fromSchema, (double) value.get("amount", fromSchema));
            record.set(0, value.get("item", fromSchema));
            record.set(1, value.get("amount", fromSchema));

            context.write(new Text(name), record);
            LOG.info(record.toString());
        }
    }

    public static class AggregationReduce extends Reducer<Text, HCatRecord, WritableComparable, HCatRecord> {
        private HCatSchema toSchema;

        @Override
        public void setup(Context context) throws IOException {
            toSchema = HCatOutputFormat.getTableSchema(context.getConfiguration());
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

            HCatRecord record = new DefaultHCatRecord(5);
            record.set("name", toSchema, key.toString());
            record.set("item_num", toSchema, (long) items.size());
            record.set("sum_amount", toSchema, sumAmount);
            record.set("avg_amount", toSchema, sumAmount / cnt);
            record.set("max_amount", toSchema, maxAmount);
            // record.set(0, key.toString());
            // record.set(1, (long) items.size());
            // record.set(2, sumAmount);
            // record.set(3, sumAmount/cnt);
            // record.set(4, minAmount);
            context.write(null, record);
            LOG.info(record.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getConf();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Group By <fromTable> <toTable> <isTruncate>");
            System.exit(2);
        }
        // String fromTable = otherArgs[0];
        // String toTable = otherArgs[1];
        String fromTable = "tmp.mr_group";
        String toTable = "tmp.mr_group_result";
        String fromDbName = fromTable.split("\\.")[0];
        String fromTbName = fromTable.split("\\.")[1];
        String toDbName = toTable.split("\\.")[0];
        String toTbName = toTable.split("\\.")[1];


        Job job = Job.getInstance(conf, "UseHCatAggregation");
        job.setNumReduceTasks(1);

        if (HadoopUtil.isDevelopment()) {
            job.setJar(HadoopUtil.LOCAL_JAR_NAME);
        } else {
            job.setJarByClass(Aggregation.class);
        }

        job.setMapperClass(AggregationMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);  // 必须DefaultHCatRecord，否则报错Error: java.io.IOException: Type mismatch in value from map: expected org.apache.hive.hcatalog.data.HCatRecord, received org.apache.hive.hcatalog.data.DefaultHCatRecord

        job.setReducerClass(AggregationReduce.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(HCatRecord.class);

        // HCatInputFormat.setInput(job, "tmp", "mr_group");
        HCatInputFormat.setInput(job, fromDbName, fromTbName);
        job.setInputFormatClass(HCatInputFormat.class);

        // HCatOutputFormat.setOutput(job, OutputJobInfo.create("tmp", "mr_group_result", null));
        HCatOutputFormat.setOutput(job, OutputJobInfo.create(toDbName, toTbName, null));
        job.setOutputFormatClass(HCatOutputFormat.class);
        HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
        HCatOutputFormat.setSchema(job, s);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
