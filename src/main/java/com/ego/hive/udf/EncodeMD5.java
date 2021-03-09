package com.ego.hive.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;


public class EncodeMD5 extends UDF {

    public String evaluate(String str) {
        if (str == null || str.trim().equals("")) {
            return null;
        }
        try {
            // 编码// return Base64.getEncoder().encodeToString(str.getBytes("utf-8"));
            // return Hashing.md5().hashString(str).toString();  // deprecated, guava高版本已经删除
            return DigestUtils.md5Hex(str);
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
        for (String s : list) {
            System.out.println(dbMD5.evaluate(s));
        }
    }
}
