package com.ego.mr.datasync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * load mysql table import HDFS
 * <p>
 * CREATE TABLE `users` (
 * `id` int(11) NOT NULL AUTO_INCREMENT,
 * `name` varchar(30) DEFAULT NULL,
 * `age` int(11) DEFAULT NULL,
 * `balance` double(18,2) DEFAULT NULL COMMENT '余额',
 * `create_time` timestamp NULL DEFAULT NULL,
 * `upload_date` date DEFAULT NULL,
 * `dt` datetime DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * <p>
 * INSERT INTO `users` VALUES (1, '张三', NULL, 88.20, '2019-10-01 21:36:19', NULL, NULL);
 * INSERT INTO `users` VALUES (2, 'xiaoming', 18, 66.00, '2019-11-02 21:36:48', '2019-11-04', '2019-11-02 21:40:29');
 * INSERT INTO `users` VALUES (3, 'mic', 24, 38.63, NULL, '2019-11-02', NULL);
 * INSERT INTO `users` VALUES (4, '北京 上海', NULL, 100.00, NULL, NULL, '2019-11-02 21:40:11');
 * INSERT INTO `users` VALUES (5, 'unknown', NULL, NULL, NULL, NULL, NULL);
 * INSERT INTO `users` VALUES (6, NULL, NULL, NULL, NULL, NULL, NULL);
 * <p>
 * drop table tmp.mysql_users;
 * create external table tmp.mysql_users (
 * id int,
 * name string,
 * age int,
 * balance double,
 * create_time string,
 * upload_date string,
 * dt string
 * )
 * row format delimited
 * fields terminated by '\t'
 * location '/user/work/tmp/mysql_users';
 * <p>
 * select * from tmp.mysql_users;
 * <p>
 * 推荐使用hive的默认分隔符"\001"作为字段分隔符
 */


public class DBToHDFS {

    public static class TableUsers implements Writable, DBWritable {
        /**
         * 全用字符串来处理，txt格式本来也没有字段类型定义，在hive中定义成相应的数据格式即可。
         * <p>
         * ResultSet.getInt，ResultSet.getLong，ResultSet.getDouble等
         * 如果获取的数据库的值是null，默认会返回0。如果判断设置为null会报空指针的错误，处理起来非常麻烦！！！
         * <p>
         * ResultSet.getString会把null值替换成"null"字符串，如果想替换成hive的空值字符，需要转换成"\N"
         * 相当于sqoop下面参数的功能
         * --null-string '\\N' \
         * --null-non-string '\\N' \
         * <p>
         * --hive-drop-import-delims: Drop Hive record \0x01 and row delimiters (\n\r) from imported string fields
         * --hive-delims-replacement <arg> : Replace Hive record \0x01 and row delimiters (\n\r) from imported string fields with user-defined string
         * 使用方法，
         * 1、在原有sqoop语句中添加 --hive-delims-replacement " " 可以将如mysql中取到的\n, \r, and \01等特殊字符替换为自定义的字符，此处用了空格
         * 2、在原有sqoop语句中添加 --hive-drop-import-delims 可以将如mysql中取到的\n, \r, and \01等特殊字符丢弃
         * \0x01 = \u0001 = \001 ?
         * <p>
         * 能否转成成动态类，每个表都写个Bean类太麻烦了
         */

        private static final String SEP = "\t";
        private static final String HIVE_NULL_CHAR = "\\N";
        // private static final String HIVE_NULL_CHAR = "null";

        private String id;
        private String name;
        private String age;
        private String balance;
        private String create_time;
        private String upload_date;
        private String dt;

        @Override
        public String toString() {
            String[] values = {
                    this.id,
                    this.name,
                    this.age,
                    this.balance,
                    this.create_time,
                    this.upload_date,
                    this.dt
            };
            return String.join(SEP, values);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, this.id);
            Text.writeString(out, this.name);
            Text.writeString(out, this.age);
            Text.writeString(out, this.balance);
            Text.writeString(out, this.create_time);
            Text.writeString(out, this.upload_date);
            Text.writeString(out, this.dt);
            System.out.println("HDFS write: " + this.toString());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = Text.readString(in);
            this.name = Text.readString(in);
            this.age = Text.readString(in);
            this.balance = Text.readString(in);
            this.create_time = Text.readString(in);
            this.upload_date = Text.readString(in);
            this.dt = Text.readString(in);
            System.out.println("HDFS read: " + this.toString());
        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1, this.id);
            preparedStatement.setString(2, this.name);
            preparedStatement.setString(3, this.age);
            preparedStatement.setString(4, this.balance);
            preparedStatement.setString(5, this.create_time);
            preparedStatement.setString(6, this.upload_date);
            preparedStatement.setString(7, this.dt);
            System.out.println("DB write: " + this.toString());
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            // 有的把null转换成\N是为了对比测试，应该都将null转成成\N
            this.id = resultSet.getString(1);
            this.name = resultSet.getString(2);
            this.age = resultSet.getString(3) == null ? HIVE_NULL_CHAR : resultSet.getString(3);
            this.balance = resultSet.getString(4);
            this.create_time = resultSet.getString(5);
            this.upload_date = resultSet.getString(6);
            this.dt = resultSet.getString(7) == null ? HIVE_NULL_CHAR : resultSet.getString(7);
            System.out.println("DB read: " + this.toString());
        }
    }

    public static class DBInputMapper extends Mapper<LongWritable, TableUsers, Text, NullWritable> {

        public void map(LongWritable key, TableUsers value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        Configuration conf = new Configuration();
        // 如果要从windows系统中运行这个job提交客户端的程序，则需要加这个跨平台提交的参数
        conf.set("mapreduce.app-submission.cross-platform", "true");

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop-dev04:3306/repository?characterEncoding=utf8&useSSL=false",
                "todo",
                "todolist");

        String output_str = "tmp/mysql_users";
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(output_str);
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
            job.setJarByClass(DBToHDFS.class);
        }

        job.setMapperClass(DBInputMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        String[] fields = {"id", "name", "age", "balance", "create_time", "upload_date", "dt"};
        // 可以采用query，需要指定查询语句inoutQuery和计数语句inoutCountQuery。
        // DBInputFormat.setInput(job, TableUsers.class, "select id,name,balance from users", "select count(*) from users");
        DBInputFormat.setInput(job, TableUsers.class, "users", null, null, fields);
        FileOutputFormat.setOutputPath(job, new Path(output_str));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
