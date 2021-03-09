package com.ego;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HadoopUtil {

    public final static String LOCAL_JAR_NAME = "target/bigdata-1.0-SNAPSHOT.jar";
    public final static String WAREHOUSE_DIR = "/user/hive/warehouse";

    public static boolean isDevelopment() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String ip = InetAddress.getLocalHost().getHostAddress();
            // System.out.println(hostname);
            // System.out.println(ip);
            return !hostname.contains("hadoop");
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void setEnvironment() {
        if (isDevelopment()) {
            System.setProperty("HADOOP_USER_NAME", "work");
            // 不添加yarn-site.xml等文件，使用本地模式运行时需要设置hadoop.home.dir路径
            // Could not locate executablenull\bin\winutils.exe in the Hadoop binaries。Windows下的特殊配置
            // System.setProperty("hadoop.home.dir", "E:\\Application\\hadoop-2.6.0");
            System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");
        }
    }

    private static Map<String, String> getConfMap() {
        // 读取配置文件
        String file;
        if (isDevelopment()) {
            file = "conf/hadoop_development.properties";
        } else {
            file = "conf/hadoop_production.properties";
        }

        Map<String, String> confMap = new HashMap<>();
        try {
            // InputStream in = new FileInputStream(file);
            InputStream in = HadoopUtil.class.getClassLoader().getResourceAsStream(file);
            Properties prop = new Properties();
            prop.load(in);
            for (Object key : prop.keySet()) {
                confMap.put(key.toString(), prop.get(key).toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return confMap;
    }

    public static void deletePath(Configuration conf, Path path) throws IOException {
        // 删除output路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("output path: hdfs://" + path + " is deleted");
        }
    }

    public static void addTmpJars(Job job) {
        Map<String, String> mapConf = getConfMap();
        try {
            String tmpJarsPathKey = "tmp.jars.path";
            if (mapConf.containsKey(tmpJarsPathKey)) {
                job.addArchiveToClassPath(new Path(mapConf.get(tmpJarsPathKey)));
                System.out.println("Add hdfs:" + mapConf.get(tmpJarsPathKey) + " to Class Path");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // mapreduce
    public static Configuration getConf() {
        // 设置环境变量
        setEnvironment();

        Configuration conf = new Configuration();
        Map<String, String> mapConf = getConfMap();
        for (String k : mapConf.keySet()) {
            conf.set(k, mapConf.get(k));
        }
        if (isDevelopment()) {
            // yarn.application.classpath default value:
            // $HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*
            conf.set("yarn.application.classpath", "$HADOOP_CLIENT_CONF_DIR"
                    + ",$HADOOP_CONF_DIR"
                    + ",$HADOOP_COMMON_HOME/*"
                    + ",$HADOOP_COMMON_HOME/lib/*"
                    + ",$HADOOP_HDFS_HOME/*"
                    + ",$HADOOP_HDFS_HOME/lib/*"
                    + ",$HADOOP_YARN_HOME/*"
                    + ",$HADOOP_YARN_HOME/lib/*"
                    + ",/opt/cloudera/parcels/CDH/lib/hive/lib/*"
                    + ",/opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog/*");
            System.out.println("yarn.application.classpath=" + conf.get("yarn.application.classpath"));
        }
        return conf;
    }

    // spark conf
    public static SparkConf getSparkConf() {
        // 设置环境变量
        setEnvironment();

        SparkConf sparkConf = new SparkConf();

        Map<String, String> mapConf = getConfMap();
        for (String k : mapConf.keySet()) {
            sparkConf.set(k, mapConf.get(k));
        }

        return sparkConf;
    }

    // spark session
    public static SparkSession createSparkSession(String appName) {
        // 设置环境变量
        setEnvironment();

        SparkSession spark;
        if (isDevelopment()) {
            spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName(appName)
                    .config("spark.some.config.option", "some-value")
                    .enableHiveSupport()
                    .getOrCreate();
        } else {
            spark = SparkSession
                    .builder()
                    .appName(appName)
                    .config("spark.some.config.option", "some-value")
                    .enableHiveSupport()
                    .getOrCreate();
        }
        // spark.conf().set("spark.some.config.option", "some-value");
        Map<String, String> mapConf = getConfMap();
        for (String k : mapConf.keySet()) {
            spark.conf().set(k, mapConf.get(k));
        }
        return spark;
    }


    public static void main(String[] args) {
        isDevelopment();
        Map<String, String> map = HadoopUtil.getConfMap();
        for (String k : map.keySet()) {
            System.out.println(k + "=" + map.get(k));
        }
    }
}
