package com.ego.examples;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Hello world!
 */
public class HelloWorld {

    private static void calculate() {
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startDT = new Date();
        System.out.println(ft.format(startDT));

        Calendar startTime = Calendar.getInstance();
        System.out.println(ft.format(startTime.getTime()));

        // for (int i = 0; i < 10000; i++) {
        //     int sum = 0;
        //     for (int j = 0; j < 10000; j++) {
        //         sum += j;
        //     }
        //     System.out.println(i + ": " + sum);
        // }

        long cnt = 0;
        for (int i = 0; i < 10000 * 10000; i++) {
            cnt += i;
        }
        System.out.println(cnt);

        Calendar endTime = Calendar.getInstance();
        System.out.println(ft.format(endTime.getTime()));
        System.out.println("Total spent: " + (endTime.getTimeInMillis() - startTime.getTimeInMillis()) / 1000 + "s");

        System.out.println(ft.format(new Date()));
    }

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
        System.out.println((float) x / (float) y);  // 推荐
        System.out.println((float) x / y);
        System.out.println(z);
        System.out.println(String.format("%.2f", (float) x / y));


        calculate();
    }
}
