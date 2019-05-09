package com.ego.hive.udf;

import java.util.Base64;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Hello world!
 *
 */
public class EncodeBase64 extends UDF {

	public String evaluate(String str) {
		if (str == null || str.trim().equals("")) {
			return null;
		}
		try {
			// 编码
			return Base64.getEncoder().encodeToString(str.getBytes("utf-8"));
		} catch (Exception e) {
			e.printStackTrace();
			return "Base64加密失败";
		}
	}

	public static void main(String[] args) {
		String str = "Hello World!";
		EncodeBase64 eb64 = new EncodeBase64();
		System.out.println(eb64.evaluate(str));
	}
}
