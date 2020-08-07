package com.ego.examples;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
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

    private static void dtTest() {
        System.out.println("-------------------------");
        LocalDateTime dt = LocalDateTime.now();
        System.out.println(dt.getClass());
        System.out.println(dt);
        System.out.println(Timestamp.valueOf(LocalDateTime.now()));  // 强力推荐
        System.out.println(LocalDate.now());
        System.out.println(LocalTime.now());
        System.out.println("-------------------------");
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

        // set
        Set<String> set = new HashSet<>();
        System.out.println(set.add("abc")); // true
        System.out.println(set.add("xyz")); // true
        System.out.println(set.add("xyz")); // false，添加失败，因为元素已存在
        System.out.println(set.contains("xyz")); // true，元素存在
        System.out.println(set.contains("XYZ")); // false，元素不存在
        System.out.println(set.remove("hello")); // false，删除失败，因为元素不存在
        System.out.println(set.size()); // 2，一共两个元素

        calculate();

        Timestamp ts = Timestamp.valueOf("2019-01-01 12:23:44");
        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        System.out.println(now);
        System.out.println(ts.toString().substring(0, 10));

        dtTest();
    }
}
