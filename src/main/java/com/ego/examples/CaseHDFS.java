package com.ego.examples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;

public class CaseHDFS {

    private static void uploadLocalFile(FileSystem fs, String src, String dst) throws IOException {
        InputStream in = new FileInputStream(src);
        FSDataOutputStream out = fs.create(new Path(dst));
        IOUtils.copyBytes(in, out, 4096, false);
        System.out.println("uploadLocalFile");
    }

    private static void downloadLocalFile(FileSystem fs, String src, String dst) throws IOException {
        FSDataInputStream in = fs.open(new Path(src));
        OutputStream out = new FileOutputStream(dst);
        IOUtils.copyBytes(in, out, 4096, false);
        System.out.println("downloadLocalFile");
    }

    private static void catHDFSFile(FileSystem fs, String remote) throws IOException {
        System.out.println("");
        FSDataInputStream in = fs.open(new Path(remote));
        // true - 是否关闭数据流，如果是false，就在finally里关掉
        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "work");
        // 不添加yarn-site.xml等文件，使用本地模式运行时需要设置hadoop.home.dir路径
        // System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-2.6.0");
        // Could not locate executablenull\bin\winutils.exe in the Hadoop binaries。Windows下的特殊配置
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.getUsed());

        // 上传和下载尽量都写到文件名
        String dataPath = "E:/Codes/Java/big-data/data/";
        String local = dataPath + "table.csv";
        String remote = "tmp/table/table.csv";

        // 上传本地文件到HDFS
        // Path src = new Path(local);
        // Path dst = new Path(remote);
        // fs.copyFromLocalFile(src, dst);
        // FileStatus[] files = fs.listStatus(dst);
        // for (FileStatus file : files) {
        //     System.out.println(file.getPath());
        // }
        // System.out.println("copyFromLocalFile done");

        // 下载HDFS文件到本地。useRawLocalFileSystem为true可以防止本地生成crc文件。支持指定文件名，相当于文件重命名。
        // fs.copyToLocalFile(false, new Path("tmp/output_wordcount/part-r-00000"), new Path(dataPath), true);
        // fs.copyToLocalFile(false, new Path("tmp/output_wordcount/part-r-00000"), new Path(dataPath + "output_wordcount.txt"), true);
        // System.out.println("copyToLocalFile done");

        // InputStream、OutputStream方式上传和下载
        uploadLocalFile(fs, local, remote);
        downloadLocalFile(fs, "tmp/output_wordcount/part-r-00000", dataPath + "output_wordcount.txt");

        // cat查看文件内容，好像只能查看txt？csv无法查看？
        catHDFSFile(fs, remote);
        catHDFSFile(fs, "tmp/input_wordcount/wordcount.txt");

        // 关闭文件系统
        fs.close();
    }
}
