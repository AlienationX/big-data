package com.ego.mapreduce.driver;

import com.ego.mapreduce.mapper.WordCountMapper;
import com.ego.mapreduce.reducer.WordCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "work");
        // System.setProperty("hadoop.home.dir", "/Users/MacBook/Software/soft/CDH/hadoop-2.7.1");

        Configuration conf = new Configuration();
        // 如果要从windows系统中运行这个job提交客户端的程序，则需要加这个跨平台提交的参数
        // conf.set("mapreduce.framework.name", "yarn");
        // conf.set("mapred.remote.os", "Linux");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 如果文件位置无法识别，需要手动添加
        // conf.addResource("core-site.xml");
        // conf.addResource("hdfs-site.xml");
        // conf.addResource("mapred-site.xml");
        // conf.addResource("yarn-site.xml");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        // 删除output路径
        Path path = new Path("tmp/output_wordcount");
        // Path path = new Path(otherArgs[1]);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("output path is deleted");
        }

        Job job = Job.getInstance(conf);
        job.setJobName("Word Count");

        // Error: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class com.ego.mr.Example$TokenizerMapper not found
        // 远程提交yarn集群需要指定打包的文件，否则会报mapper、reducer类 not found
        // job.setJar("target/bigdata-1.0-SNAPSHOT.jar");
        // job.setJar("target/bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar");
        // 打包已经编译成class文件了，所以上传集群直接指定类运行即可
        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
