package com.ego.mr;

/**
 * example:
 * hadoop jar bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar com.ego.mr.WordCount tmp/word.txt tmp/output
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        // private final static IntWritable one = new IntWritable(1);
        // private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // StringTokenizer itr = new StringTokenizer(value.toString());
            // while (itr.hasMoreTokens()) {
            //     word.set(itr.nextToken());
            //     context.write(word, one);
            // }
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // result.set(sum);
            // context.write(key, result);
            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        // System.setProperty("HADOOP_USER_NAME", "lshu");
        // System.setProperty("hadoop.home.dir", "/Users/MacBook/Software/soft/CDH/hadoop-2.7.1");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        // 删除output路径
        Path path = new Path("tmp/output");
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("output path is deleted");
        }


        // Job job = new Job(conf, "word count");   // 老代码，不推荐的，废弃的
        Job job = Job.getInstance();
        job.setJobName("word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        // ??? 这个必须设置吗，还不太清楚
        // job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
