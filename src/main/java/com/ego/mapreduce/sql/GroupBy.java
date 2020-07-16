package com.ego.mapreduce.sql;


import com.ego.HadoopUtil;
import com.ego.mapreduce.WordCount;
import com.ego.mapreduce.WordCountES;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;

import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.log4j.Logger;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * create table tmp.mr_group stored as parquet as
 * select 'roger' as name,'2020-01-01' as dt,'milk' as item,5 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'milk' as item,5 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'bread' as item,10 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'orange' as item,3 as amount union all
 * select 'mike' as name,'2020-01-01' as dt,'bread' as item,12.5 as amount union all
 * select 'mike' as name,'2020-01-02' as dt,'orange' as item,6 as amount union all
 * select 'mike' as name,'2020-01-03' as dt,'orange' as item,6 as amount;
 * <p>
 * select name,
 * count(distinct item) as item_num,
 * sum(amount) as sum_amount,
 * avg(amount) as avg_amount,
 * max(amount) as min_amount
 * from tmp.mr_group
 * group by name;
 */

public class GroupBy {
    private static Logger LOG = Logger.getLogger(GroupBy.class);


    public static class GroupMap extends Mapper<WritableComparable, HCatRecord, Object, HCatRecord> {
        private HCatSchema fromSchema;

        @Override
        public void setup(Context context) throws IOException {
            fromSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws InterruptedException, IOException {
            String name = value.get("name", fromSchema).toString();

            HCatRecord record = new DefaultHCatRecord();


            record.setString("item", fromSchema,value.get("item", fromSchema).toString());
            record.setDouble("amount", fromSchema, (double) value.get("amount",fromSchema));
            context.write(new Text(name), record);
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


            Iterator<HCatRecord> iter = values.iterator();
            while (iter.hasNext()){

            }
            HCatRecord record = new DefaultHCatRecord();
            // record.set("name", schema,);
            context.write(null, record);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getConf();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Group By <fromTable> <targetTable>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "GroupBy");
        // job.setNumReduceTasks(1);
        // HadoopUtil.addTmpJars(job);

        if (HadoopUtil.isDevelopment()) {
            job.setJar(HadoopUtil.LOCAL_JAR_NAME);
        } else {
            job.setJarByClass(GroupBy.class);
        }

        job.setMapperClass(GroupBy.GroupMap.class);
        job.setReducerClass(GroupBy.GroupReduce.class);

        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        // job.setOutputValueClass(HCatRecord.class);

        HCatInputFormat.setInput(job,"tmp","mr_group");
        job.setInputFormatClass(HCatInputFormat.class);

        HCatOutputFormat.setOutput(job, OutputJobInfo.create("tmp","mr_group_result",null));
        job.setOutputFormatClass(HCatOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
