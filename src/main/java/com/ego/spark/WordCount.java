// package com.hillinsight.spark;
//
// import java.util.Properties;
//
// import org.apache.log4j.Logger;
// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.sql.DataFrame;
// import org.apache.spark.sql.SQLContext;
// import org.apache.spark.sql.SaveMode;
// import org.apache.spark.sql.hive.HiveContext;
//
// /**
// * Hello world!
// */
// class DBConfig {
//
//    private String url;
//    private Properties props = new Properties();
//
//    public DBConfig(String driver, String url, String user, String password) {
//        this.url = url;
//        this.props.put("driver", driver);
//        this.props.put("user", user);
//        this.props.put("password", password);
//    }
// }
//
// public class WordCount {
//
//    public static Logger logger = Logger.getLogger("");
//    public JavaSparkContext ctx;
//    public HiveContext hiveContext;
//    public SQLContext sqlContext;
//    public DBConfig mssqlConf;
//    public DBConfig mysqlConf;
//    // private Map<String, String> mysqlConf;
//    // private Map<String, String> mssqlConf;
//
//    public WordCount() {
//        SparkConf sparkConf = new SparkConf().setAppName("Spark Hello World").setMaster("yarn-client");
//        // sparkConf.set("spark.yarn.dist.files", "src/main/java/yarn-site.xml");
//        // sparkConf.set("spark.driver.host", "127.0.0.1");
//        // sparkConf.set("spark.yarn.preserve.staging.files", "false");
//        // sparkConf.set("yarn.resourcemanager.hostname", "hi-prod-09.hillinsight.com");
//        // sparkConf.set("spark.ui.port", "36000");
//        System.out.println("A");
//        // 设置运行资源参数
//        // sparkConf.set("spark.executor.instances", "30");
//        // sparkConf.set("spark.executor.memory", "5G");
//        // sparkConf.set("spark.driver.memory", "3G");
//        // sparkConf.set("spark.driver.maxResultSize", "10G");
//        sparkConf.set("spark.yarn.jar", "hdfs://hi-prod-09.hillinsight.com:8020/user/lshu/tmp/spark-assembly-1.6.0-cdh5.7.1-hadoop2.6.0-cdh5.7.1.jar");
//        ctx = new JavaSparkContext(sparkConf);
//        // ctx.addJar("/Users/MacBook/Documents/workspace/BigData/target/spark-hello-world-jar-with-dependencies.jar");
//        hiveContext = new HiveContext(ctx);
//        sqlContext = new SQLContext(ctx);
//        System.out.println("B");
//        dbInit();
//    }
//
//    private void dbInit() {
//        // mssqlConf = new HashMap<String, String>();
//        // // com.microsoft.sqlserver.jdbc.SQLServerDriver（2005版本及以后）
//        // // com.microsoft.jdbc.sqlserver.SQLServerDriver（2000版本）
//        // mssqlConf.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"); // 必须填加 否则报错 java.sql.SQLException:No suitable driver
//        // mssqlConf.put("url", "jdbc:sqlserver://10.15.1.11:2121;database=PMS");
//        // mssqlConf.put("user", "warmsoft_read");
//        // mssqlConf.put("password", "Awq123456");
//
//        mssqlConf = new DBConfig("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://10.15.1.11:2121;database=PMS", "warmsoft_read", "Awq123456");
//        mysqlConf = new DBConfig("com.mysql.jdbc.Driver", "jdbc:mysql://10.15.1.14:3306/xiaonuan?useUnicode=true&characterEncoding=utf-8", "work", "phkAmwrF");
//    }
//
//    public void showMysqlData() {
//        String dbTable = "syspmsusers";
//        // mysqlConf.put("dbtable", dbTable);
//        try {
//            DataFrame df = sqlContext.read().jdbc(this.mssqlConf.url, dbTable, this.mssqlConf.props);
//            // 支持sql查询出的结果集
//            // DataFrame df = sqlContext.read().jdbc(mssqlConf.url, "(select top 2 orgid,name from syspmsusers) as T", mssqlConf.props);
//            // DataFrame df = sqlContext.read().jdbc(mssqlConf.url, "(select top 2 s.id as orgid,t.name from syspmsusers t inner join pclientmanagement s on
//            // t.orgid=s.id) as T", mssqlConf.props);
//            // DataFrame df = sqlContext.read().format("jdbc").options(mysqlConf).load();
//            System.out.println("=============================================> 表的总记录数：" + df.count());
//            // df.cache();
//            df.show();
//            System.out.println("=============================================> DataFrame hend 2");
//            df.head(2);
//            df.printSchema();
//            System.out.println("=============================================> Select limit 10 & 5");
//            df.describe();
//            df.registerTempTable("mysql_tb");
//            DataFrame tmpDF = sqlContext.sql("select * from mysql_tb limit 10");
//            tmpDF.show();
//            tmpDF.limit(5).show();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void rdb2Hive() {
//        String fromTable = "IInstoreRecipe";
//        String toTable = "aibi.IInstoreRecipes_hive";
//        // mssqlConf.put("dbtable", fromTable);
//        try {
//            // DataFrame jdbcDF = hiveContext.read().format("jdbc").options(mssqlConf).load();
//            DataFrame jdbcDF = hiveContext.read().jdbc(this.mssqlConf.url, fromTable, this.mssqlConf.props);
//            // 如果支持以下方式，不知道能不能省去读入内存的时间直接保存。
//            // hiveContext.read().jdbc(mssqlConf.url, fromTable, mssqlConf.props).write().format("orc").mode(SaveMode.Overwrite).save(toTable);
//            System.out.println("=============================================> 表的总记录数：" + jdbcDF.count());
//            jdbcDF.show(5);
//            jdbcDF.registerTempTable("tmp");
//            jdbcDF.printSchema();
//            hiveContext.sql("drop table " + toTable);
//            hiveContext.sql("create table " + toTable + " as select * from tmp");
//            System.out.println("=============================================> create table successful !");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * saveAsTable该方法在CDH中可能存在问题
//     */
//    // public void rdb2HDFS() {
//    // System.out.println("=============================================> rdb2HDFS start");
//    // String fromTable = "syspmsusers";
//    // String toTable = "aibi.syspmsusers_hdfs";
//    // mssqlConf.put("dbtable", fromTable);
//    // try {
//    // DataFrame df = hiveContext.read().format("jdbc").options(mssqlConf).load();
//    // df.printSchema();
//    // // df.registerTempTable("tmp");
//    // // hiveContext.sql("drop table " + toTable);
//    // // hiveContext.sql("create table " + toTable + " as select * from tmp where 1=0");
//    // // df.write().mode(SaveMode.Overwrite).saveAsTable(toTable); // 表需要提前创建
//    // // df.write().format("orc").mode(SaveMode.Overwrite).saveAsTable(toTable);
//    // df.write().format("orc").mode(SaveMode.Overwrite).save(toTable);
//    // } catch (Exception e) {
//    // System.out.println("=============================================> rdb2HDFS error");
//    // e.printStackTrace();
//    // }
//    // }
//    public void hive2Mysql() {
//        String toTable = "lshu_syspmsusers";
//        try {
//            String query = "select * from data_xiaonuan_final.syspmsusers limit 999";
//            DataFrame df = hiveContext.sql(query);
//            df.printSchema();
//            df.write().mode(SaveMode.Append).jdbc(this.mysqlConf.url, toTable, this.mysqlConf.props); // 这种方法是可以的
//            // df.write().format("jdbc").options(mysqlConf).save(); // 这个方法不知道哪里有问题，会报错...
//        } catch (Exception e) {
//            System.out.println("=============================================> hive2Mysql error");
//            e.printStackTrace();
//        }
//    }
//
//    public void Mssql2Mysql() {
//        String fromTable = "syspmsusers";
//        String toTable = "lshu_syspmsusers";
//        try {
//            DataFrame df = sqlContext.read().jdbc(this.mssqlConf.url, fromTable, this.mssqlConf.props).select("Name", "Id", "OrgId", "LoginName", "Pwd", "RoleId", "IDNumber", "Gender", "BirthDate", "PhoneNumber", "Status", "CreatingDate",
//                    "LastLoginDate", "DefaultCard", "OpenId", "UpdateStamp", "ImgName");
//            // 两张表的字段需要一致，源数据的表可以比目标表少字段，但是不能多。字段顺序可以不一致。
//            df.show(5);
//            df.printSchema();
//            // SaveMode.Overwrite 如果表存在，会drop掉，切记小心。所以强烈不建议用Overwrite！
//            // SaveMode.Ignore 是更新，好像需要主键，也不太确定是否插入新数据还是只更新数据???
//            df.limit(999).write().mode(SaveMode.Append).jdbc(mysqlConf.url, toTable, mysqlConf.props); // 没有表会自动创建表，字段映射可能存在问题，字符串就会映射成text，建议提前创建表
//        } catch (Exception e) {
//            System.out.println("=============================================> Mssql2Mysql error");
//            e.printStackTrace();
//        }
//    }
//
//    public static void main(String[] args) {
//        System.out.println("Hello World!");
//        logger.info("I am logger...");
//        // System.setProperty("HADOOP_USER_NAME", "lshu");
//        WordCount wordCount = new WordCount();
//        System.out.println("=============================================> showMysqlData start");
//        // wordCount.showMysqlData();
//        System.out.println("=============================================> rdb2Hive start");
//        wordCount.rdb2Hive();
//        System.out.println("=============================================> rdb2HDFS start");
//        // wordCount.rdb2HDFS();
//        System.out.println("=============================================> hive2Mysql start");
//        // wordCount.hive2Mysql();
//        System.out.println("=============================================> Mssql2Mysql start");
//        // wordCount.Mssql2Mysql();
//        // wordCount.ctx.stop();
//
//        // SparkConf conf = new SparkConf().setAppName("JDBCSparkSqlTest").set("spark.storage.memoryFraction", "0.4") //
//        // 降低cache操作的内存占比
//        // .set("spark.shuffle.consolidateFiles", "true") // shuffle合并map端输出文件,你有几个cpu core就有几份文件
//        // .set("spark.shuffle.file.buffer", "64") // map端内存缓冲64kb
//        // .set("spark.shuffle.memoryFraction", "0.3") // reduce端内存占比
//        // // .set("spark.reducer.maxSizeInFlight", "24") // reduce端缓存大小，默认是48M，牺牲性能换取可以正常运行
//        // .set("spark.shuffle.io.maxRetries", "60") // shuffle文件拉取，没有拉取到，重试的次数60次，默认是3次
//        // .set("spark.shuffle.io.retryWait", "60") // shuffle文件拉取，没有拉取到每次
//        // .setMaster("local[2]");
//        // JavaSparkContext sc = new JavaSparkContext(conf);
//        // SQLContext sqlContext = new SQLContext(sc);
//        //
//        // Map<String, String> options = new HashMap<String, String>();
//        // options.put("driver", "com.mysql.jdbc.Driver");
//        // options.put("url", "jdbc:mysql://10.15.1.14:3306/xiaonuan");
//        // options.put("user", "warmsoft_read");
//        // options.put("password", "phkAmwrF");
//        // options.put("dbtable", "lshu_syspmsusers");
//        //
//        // DataFrame df_belle_comparison_online2offline_new = sqlContext.read().format("jdbc").options(options).load();
//        // df_belle_comparison_online2offline_new.registerTempTable("tmp");
//        // DataFrame df = sqlContext.sql("select count(*) from tmp");
//        // df.show(500);
//    }
// }
