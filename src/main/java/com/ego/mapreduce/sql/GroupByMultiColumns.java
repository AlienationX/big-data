package com.ego.mapreduce.sql;

import com.ego.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
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

/**
 * create table tmp.mr_detail_multi_keys stored as parquet as
 * select date_format(charge_time,'yyyy-MM-dd') as dt,
 *        item_id,
 *        item_name,
 *        sum(amount) as amount_sum,
 *        count(1) as cnt
 * from tmp.mr_detail
 * group by date_format(charge_time,'yyyy-MM-dd'),
 *          item_id,
 *          item_name
 * having count(*)>3;
 */


public class GroupByMultiColumns {
    private static Logger LOG = Logger.getLogger(GroupByMultiColumns.class);

    public static class MultiColumnsMapper extends Mapper<WritableComparable, HCatRecord, SortedMapWritable, HCatRecord> {
        private HCatSchema fromSchema;
        // private HCatRecord keyRecord = new DefaultHCatRecord(3);
        private SortedMapWritable keyRecord = new SortedMapWritable();
        private HCatRecord valueRecord = new DefaultHCatRecord(2);

        @Override
        public void setup(Context context) throws IOException {
            fromSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
            // keyRecord.set(0, value.getTimestamp("charge_time", fromSchema).toString().substring(0, 10));
            // keyRecord.set(1, value.getInteger("item_id", fromSchema));
            // keyRecord.set(2, value.getString("item_name", fromSchema));
            keyRecord.put(new Text("dt"),new Text(value.getTimestamp("charge_time", fromSchema).toString().substring(0, 10)));
            keyRecord.put(new Text("item_id"),new IntWritable(value.getInteger("item_id", fromSchema)));
            keyRecord.put(new Text("item_name"),new Text(value.getString("item_name", fromSchema)));

            valueRecord.set(0, value.getDouble("amount", fromSchema));

            LOG.info("key: " + keyRecord.toString() + "value: " + valueRecord.toString());
            context.write(keyRecord, valueRecord);
        }

    }

    public static class MultiColumnsReducer extends Reducer<SortedMapWritable, HCatRecord, WritableComparable, HCatRecord> {
        private HCatSchema toSchema;
        private HCatRecord valueRecord = new DefaultHCatRecord(5);
        private Double sumAmount = null;
        private int cnt = 0;

        @Override
        public void setup(Context context) throws IOException {
            toSchema = HCatOutputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void reduce(SortedMapWritable key, Iterable<HCatRecord> values, Context context) throws IOException, InterruptedException {
            for (HCatRecord value : values) {
                Object o = value.get(0);
                if (o != null) {
                    Double amount = Double.parseDouble(o.toString());
                    if (sumAmount == null) {
                        sumAmount = amount;
                    } else {
                        sumAmount += amount;
                    }
                }
                cnt += 1;
            }

            valueRecord.set("dt", toSchema, key.get(new Text("dt")));
            valueRecord.set("item_id", toSchema, key.get("item_id"));
            valueRecord.set("item_name", toSchema, key.get("item_name"));
            valueRecord.set("sum_amount", toSchema, sumAmount);
            valueRecord.set("cnt", toSchema, cnt);
            context.write(null, valueRecord);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getConf();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Group By Multi Keys <fromTable> <toTable> <isTruncate>");
            System.exit(2);
        }
        // String fromTable = otherArgs[0];
        // String toTable = otherArgs[1];
        String fromTable = "tmp.mr_detail";
        String toTable = "tmp.mr_detail_multi_keys";

        String p = "\\.";
        String fromDbName = fromTable.split(p)[0];
        String fromTbName = fromTable.split(p)[1];
        String toDbName = toTable.split(p)[0];
        String toTbName = toTable.split(p)[1];

        Job job = Job.getInstance(conf, "Group By Multi Keys");
        job.setNumReduceTasks(1);

        if (HadoopUtil.isDevelopment()) {
            job.setJar(HadoopUtil.LOCAL_JAR_NAME);
        } else {
            job.setJarByClass(GroupByMultiColumns.class);
        }

        job.setMapperClass(GroupByMultiColumns.MultiColumnsMapper.class);
        job.setMapOutputKeyClass(SortedMapWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);  // 必须DefaultHCatRecord，否则报错Error: java.io.IOException: Type mismatch in value from map: expected org.apache.hive.hcatalog.data.HCatRecord, received org.apache.hive.hcatalog.data.DefaultHCatRecord

        job.setReducerClass(GroupByMultiColumns.MultiColumnsReducer.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(HCatRecord.class);

        HCatInputFormat.setInput(job, fromDbName, fromTbName);
        job.setInputFormatClass(HCatInputFormat.class);

        HCatOutputFormat.setOutput(job, OutputJobInfo.create(toDbName, toTbName, null));
        job.setOutputFormatClass(HCatOutputFormat.class);
        HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
        HCatOutputFormat.setSchema(job, s);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
