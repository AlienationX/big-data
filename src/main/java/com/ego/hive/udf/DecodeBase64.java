package com.ego.hive.udf;

import java.util.Base64;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Hello world!
 *
 */
public class DecodeBase64 extends UDF {

	public String evaluate(String str) {
		if (str == null || str.trim().equals("")) {
			return null;
		}
		try {
			// 解码
			byte[] asBytes = Base64.getDecoder().decode(str);
			return new String(asBytes, "utf-8");
		} catch (Exception e) {
			e.printStackTrace();
			// System.out.println(e.getMessage());
			return "Base64解密失败";
		}
	}

	public static void main(String[] args) {
		String str = "SGVsbG8gV29ybGQh";
		DecodeBase64 db64 = new DecodeBase64();
		System.out.println(db64.evaluate(str));
	}
}
