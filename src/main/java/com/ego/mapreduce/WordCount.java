package com.ego.mapreduce;

/**
 * example:
 * hadoop jar bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar com.ego.mr.WordCount tmp/input_wordcount tmp/output_wordcount
 */

import com.ego.HadoopUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount {
    private static Logger LOG = Logger.getLogger(WordCount.class);

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
                LOG.info("{\"" + word + "\": " + one + "}");
            }
            // String[] words = value.toString().replaceAll(" +", " ").split(" ");
            // for (String word : words) {
            //     if (!"".equals(word.trim())) {
            //         context.write(new Text(word), new IntWritable(1));
            //     }
            // }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // private IntWritable result = new IntWritable();

        @Override
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
        // 设置环境变量
        HadoopUtil.setEnvironment();

        Configuration conf = new Configuration();
        // 如果要从windows系统中运行这个job提交客户端的程序，则需要加这个跨平台提交的参数
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 如果文件位置无法识别，需要手动添加
        // conf.addResource("core-site.xml");
        // conf.addResource("hdfs-site.xml");
        // conf.addResource("mapred-site.xml");
        // conf.addResource("yarn-site.xml");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: word count <in> <out>");
            System.exit(2);
        }

        // 删除output路径
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(otherArgs[1]);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("output path is deleted");
        }

        Job job = Job.getInstance(conf);
        job.setJobName("WordCount");
        job.setNumReduceTasks(1);

        // Error: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class com.ego.mr.Example$TokenizerMapper not found
        // 远程提交yarn集群需要指定打包的文件，否则会报mapper、reducer类 not found
        // job.setJar("target/bigdata-1.0-SNAPSHOT.jar");
        // 打包已经编译成class文件了，所以上传集群直接指定类运行即可
        // job.setJarByClass(WordCount.class);
        if (HadoopUtil.isDevelopment()) {
            job.setJar("target/bigdata-1.0-SNAPSHOT.jar");
        } else {
            job.setJarByClass(WordCount.class);
        }

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setInputFormatClass(TextInputFormat.class); // 默认为TextInputFormat，所以TextInputFormat可以不用设置

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setOutputFormatClass(TextOutputFormat.class); // 默认为TextInputFormat，所以TextInputFormat可以不用设置

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
