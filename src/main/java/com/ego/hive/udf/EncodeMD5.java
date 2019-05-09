package com.ego.hive.udf;

import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;


public class EncodeMD5 extends UDF {

    public String evaluate(String str) {
        if (str == null || str.trim().equals("")) {
            return null;
        }
        try {
            // 编码// return Base64.getEncoder().encodeToString(str.getBytes("utf-8"));
            return Hashing.md5().hashString(str).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "MD5加密失败";
        }
    }

    public static void main(String[] args) {
        String str = "Hello World";
        EncodeMD5 dbMD5 = new EncodeMD5();
        System.out.println(dbMD5.evaluate(str));

        String[] list = "beijing shanghai guangzhou 深圳".split(" ");
        for (int i = 0; i < list.length; i++) {
            System.out.println(dbMD5.evaluate(list[i]));
        }
    }
}
