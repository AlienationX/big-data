package com.ego.examples;

import java.util.StringTokenizer;

/**
 * Hello world!
 */
public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String a = "a  b c";
        System.out.println(a.replaceAll(" +", " "));

        String str = "microsoft google   taobao facebook zhihu";
        // 以 , 号为分隔符来分隔字符串
        StringTokenizer st = new StringTokenizer(str, " ");
        while (st.hasMoreTokens()) {
            System.out.println(st.nextToken());
        }

        String[] values = {"beijing", "shanghai", "shenzhen"};
        System.out.println(String.join("\t", values));

        // 精确小数位
        int x = 2;
        int y = 3;
        float z = x / y;
        System.out.println((float) x / y);
        System.out.println(z);
        System.out.println(String.format("%.2f", (float) x / y));
    }
}
