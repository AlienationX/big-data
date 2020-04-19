package com.ego.mapreduce;

import com.ego.HadoopUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountES {

    public static class WordsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }

    }

    public static class WordsReducer extends Reducer<Text, IntWritable, Text, MapWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable result = new MapWritable();
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.put(new Text("word"), key);
            result.put(new Text("count"), new IntWritable(sum));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 设置环境变量
        HadoopUtil.setEnvironment();

        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("es.nodes", "hadoop-dev04:9200");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: elasticsearch word count <in> <out>");
            System.exit(2);
        }
        conf.set("es.resource", otherArgs[1]);

        // conf.set("es.batch.size.entries", "1"); //
        // conf.set("es.batch.write.retry.count", "3"); //  默认是重试3次,为负值的话为无限重试(慎用)
        // conf.set("es.batch.write.retry.wait", "60"); // 默认重试等待时间是10s.可适当加大
        // conf.set("es.http.timeout", "100s"); // 连接es的超时时间设置,默认1m,Connection error时可适当加大此参数

        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(1);

        if (HadoopUtil.isDevelopment()) {
            job.setJar("target/bigdata-1.0-SNAPSHOT.jar");
        } else {
            job.setJarByClass(WordCount.class);
        }
        job.setMapperClass(WordsMapper.class);
        job.setReducerClass(WordsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setOutputFormatClass(EsOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
