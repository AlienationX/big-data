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
    }
}
