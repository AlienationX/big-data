package com.ego.mapreduce.datasync;

import com.ego.HadoopUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;


public class HiveToEs {

    public static class HiveToEsMapper extends Mapper<WritableComparable, HCatRecord, Text, MapWritable> {
        public static HCatSchema schema;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            schema = HCatInputFormat.getTableSchema(conf);
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
            MapWritable result = new MapWritable();
            for (String columnName : schema.getFieldNames()) {
                result.put(new Text(columnName), (Text) (value.get(columnName, schema)));
            }
            context.write(null, result);
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = HadoopUtil.getConf();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: datasync hive to es <inputTable> <outputIndex>");
            System.exit(2);
        }

        String input = otherArgs[0];
        String indexName = otherArgs[1];

        String dbName = "default";
        String tableName;
        if (input.split(".").length == 2) {
            dbName = input.split(".")[0];
            tableName = input.split(".")[1];
        } else {
            tableName = input;
        }

        conf.set("es.resource", indexName);
        Job job = Job.getInstance(conf, "Hive To Es: " + tableName + " --> " + indexName);


        if (HadoopUtil.isDevelopment()) {
            job.setJar(HadoopUtil.LOCAL_JAR_NAME);
        } else {
            job.setJarByClass(HiveToEs.class);
        }
        job.setMapperClass(HiveToEsMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        HCatInputFormat.setInput(job, dbName, tableName);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
