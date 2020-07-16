package com.ego;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
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

    public static boolean isDevelopment() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String ip = InetAddress.getLocalHost().getHostAddress();
            // System.out.println(hostname);
            // System.out.println(ip);
            return !hostname.contains("hadoop-dev");

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
            System.setProperty("hadoop.home.dir", "E:\\Application\\hadoop-2.6.0");
        }
    }

    private static Map<String, String> getConfMap() {

        // 设置环境变量
        setEnvironment();

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

    public static void addTmpJars(Job job) {
        Map<String, String> mapConf = getConfMap();
        try {
            String tmpJarsPathKey = "tmp.jars.path";
            if (mapConf.containsKey(tmpJarsPathKey)) {
                job.addFileToClassPath(new Path(mapConf.get(tmpJarsPathKey)));
                System.out.println("Add " + mapConf.get(tmpJarsPathKey) + "to Class Path");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Configuration getConf() {
        Configuration conf = new Configuration();
        Map<String, String> mapConf = getConfMap();
        for (String k : mapConf.keySet()) {
            conf.set(k, mapConf.get(k));
        }
        return conf;
    }

    public static SparkSession createSparkSession(String appName) {
        SparkSession spark;
        if (isDevelopment()) {
            spark = SparkSession
                    .builder()
                    .master("local")
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
