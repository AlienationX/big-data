package com.ego.mapreduce.datasync;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.InetAddress;

import com.ego.HadoopUtil;
import com.ego.mapreduce.common.TableBean;

/**
 * from database import table to HDFS
 *
 * todo
 * 需要通过jdbc动态获取所有字段名称
 * 控制map数量
 *
 * param table_name  or query_sql
 * param tmp_path
 * param output_path
 */


public class DBToHDFSHard {

    public static class DBInputMapper extends Mapper<LongWritable, TableBean, Text, NullWritable> {
        Text outKey = new Text();

        public void map(LongWritable key, TableBean value, Context context) throws IOException, InterruptedException {
            String formatValue = value.toString().replaceAll("\n", " ").replaceAll("\r", " ").replaceAll("\t", " ").replaceAll("\001", "");
            outKey.set(formatValue);
            context.write(outKey, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getConf();

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop-dev04:3306/repository?characterEncoding=utf8&useSSL=false",
                "todo",
                "todolist");

        String outputStr = "tmp/mysql_users";
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outputStr);
        if (fs.exists(outPath)) {
            String absOutPath = fs.getFileStatus(outPath).getPath().toString();
            fs.delete(outPath, true);
            System.out.println(absOutPath + " had deleted");
        }

        Job job = Job.getInstance(conf);
        job.setJobName("Mysql To HDFS");
        job.addFileToClassPath(new Path("hdfs:///user/work/jars/mysql-connector-java-5.1.48.jar"));
        job.setNumReduceTasks(0);  // reduceTasks为0，就不会执行reduce阶段

        if (InetAddress.getLocalHost().getHostName().equals("Dell")) {
            job.setJar("target/bigdata-1.0-SNAPSHOT.jar");
        } else {
            job.setJarByClass(DBToHDFSHard.class);
        }

        job.setMapperClass(DBInputMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        String[] fields = {"id", "name", "age", "balance", "create_time", "upload_date", "dt"};
        // 可以采用query，需要指定查询语句inoutQuery和计数语句inoutCountQuery。
        // DBInputFormat.setInput(job, TableUsers.class, "select id,name,balance from users", "select count(*) from users");
        DBInputFormat.setInput(job, TableBean.class, "users", null, null, fields);
        FileOutputFormat.setOutputPath(job, new Path(outputStr));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // 定义临时路径，执行成功后move data到具体的表的路径下
        // private static final String DEFAULT_PATH = "/tmp/work"
        // Moving data to: hdfs://hadoop-dev01:8020/user/hive/warehouse/medical.db/dws_patient_visitarea_sum
    }

}
