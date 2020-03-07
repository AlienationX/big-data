package com.ego.examples;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

public class CaseProperties {
    /**
     * 读取配置文件的基本操作
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Properties sysProp = System.getProperties();
        sysProp.list(System.out);

        // read
        InputStream in = new FileInputStream("conf/db.properties");
        Properties prop = new Properties();
        prop.load(in);
        System.out.println("-- custom properties --");
        System.out.println("jdbc.mysql.local.username --> " + prop.getProperty("jdbc.mysql.local.username"));
        for (Object key : prop.keySet()) {
            System.out.println(key + "=" + prop.get(key));
        }

        // write
        Properties cacheProp = new Properties();
        cacheProp.setProperty("name", "xiaoming");
        cacheProp.setProperty("age", "18");
        System.out.println("-- cache properties --");
        cacheProp.list(System.out);
        cacheProp.store(new FileOutputStream("conf/cache.properties"), "cache");
        cacheProp.storeToXML(new FileOutputStream("conf/cache.xml"), "temp");
    }
}
