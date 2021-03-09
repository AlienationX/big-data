package com.ego.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class CaseExecCmd {
    public static void main(String[] args) throws IOException {
        // String cmd = "ping www.baidu.com -t";
        String cmd = "ping www.baidu.com";

        //执行命令
        Process p = Runtime.getRuntime().exec(cmd);
        //取得命令结果的输出流
        InputStream fis = p.getInputStream();
        //用一个读输出流类去读，windows使用GBK，linux使用UTF-8
        InputStreamReader isr = new InputStreamReader(fis, "GBK");
        //用缓冲器读行
        BufferedReader br = new BufferedReader(isr);
        String line;
        //直到读完为止
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        // try {
        //     Runtime rt = Runtime.getRuntime();
        //     Process proc = rt.exec(cmd);
        //     InputStream is = proc.getInputStream();  // stdout
        //     InputStream es = proc.getErrorStream();  // stderr
        //     String line;
        //     BufferedReader br;
        //     br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        //     while ((line = br.readLine()) != null) {
        //         System.out.println(">>>{}", line);
        //     }
        //     br = new BufferedReader(new InputStreamReader(es, StandardCharsets.UTF_8));
        //     while ((line = br.readLine()) != null) {
        //         System.out.println(">>>{}", line);
        //     }
        // } catch (Exception e) {
        //     System.out.println(">>>异常信息", e);
        // }

    }
}
