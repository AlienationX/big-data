package com.ego.examples;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
        System.out.println(Timestamp.valueOf(LocalDateTime.now()));  // 强力推荐，只是去掉中间的T
        System.out.println(LocalDate.now());
        System.out.println(LocalTime.now());
        LocalDateTime tomorrow = LocalDateTime.of(dt.getYear(), dt.getMonth(), dt.getDayOfMonth() + 1, 0, 0, 0);
        System.out.println(tomorrow);
        System.out.println((tomorrow.compareTo(dt)));  // 两个日期相差返回的是天
        System.out.println(dt.toEpochSecond(ZoneOffset.of("+8")));  // //获取秒数（时间戳）
        System.out.println(dt.toInstant(ZoneOffset.of("+8")).toEpochMilli());  // //获取毫秒数
        // System.out.println(tomorrow.getLong() - dt.getLong());
        System.out.println(LocalDate.of(2020,1,1));
        System.out.println(LocalDate.parse("2099-12-31"));
        System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(1598432983228L), ZoneOffset.of("+8")));
        System.out.println(LocalDateTime.ofEpochSecond(1598432983228L/1000, 0,ZoneOffset.of("+8")));
        System.out.println(LocalDateTime.parse("2099-12-31 23:59:59", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println(tomorrow.plusDays(-2));  // 日期减2天
        System.out.println(tomorrow.plusHours(6));  // 日期加6时
        System.out.println("-------------------------");
    }

    private static void containsList() {
        List<String> a = Arrays.asList("a", "b", "c"); // 这种方式使用removeAll会报错
        List<String> b = Arrays.asList("b", "c");
        System.out.println(a.containsAll(b));
        System.out.println(b.containsAll(a));

        List<String> c = new ArrayList<>(Arrays.asList("a", "c", "b"));
        List<String> d = new ArrayList<>(Arrays.asList("b", "c", "d"));
        c.removeAll(d);
        System.out.println("交集: " + c);
        c.addAll(d);
        System.out.println("(去重)并集:" + c);
        System.out.println(c.subList(0, 2));

        List<String> l1 = new ArrayList<>();
        List<String> l2 = new ArrayList<>();
        System.out.println("l1=" + l1);
        System.out.println("l2=" + l2);
        System.out.println(l1.equals(l2));
        System.out.println(l1.addAll(l2));
    }

    private static void containsInnerList() {
        List<List<String>> data1 = new ArrayList<>();
        data1.add(Arrays.asList("a", "b"));
        data1.add(Arrays.asList("b", "c"));
        data1.add(Arrays.asList("c", "a"));

        List<List<String>> data2 = new ArrayList<>();
        data2.add(Arrays.asList("a", "b"));
        data2.add(Arrays.asList("b"));
        data2.add(Arrays.asList("c", "a"));

        System.out.println(data1);
        System.out.println(data2);
        data1.retainAll(data2);  // 可以过滤
        System.out.println(data1);

        // 可以过滤
        // List<List<String>> data = new ArrayList<>();
        // for (List<String> item1: data1){
        //     for (List<String> item2 : data2) {
        //         if (item1.equals(item2)){
        //             data.add(item1);
        //         }
        //     }
        // }
        // System.out.println(data);
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

        System.out.println("110110190006221314".substring(6, 14));
        // Integer aNum = Integer.parseInt("ab");
        System.out.println(StringUtils.isNumeric("123"));
        System.out.println(LocalDate.parse("20201231", DateTimeFormatter.ofPattern("yyyyMMdd")));

        containsList();
        containsInnerList();

        System.out.println(DigestUtils.md5Hex("i love you"));

        String list = "a b c d";
        System.out.println("list: " + list.split(" ") + " len: " + list.split(" ").length);
        System.out.println("list: " + Arrays.asList(list.split(" ")) + " len: " + Arrays.asList(list.split(" ")).size());

        String s1 = "aaa,b,,,d, ,f,   ,h";
        List<String> s = new ArrayList<>(Arrays.asList(s1.split(",")));
        System.out.println(s);
        // for (String txt : s){
        //     if (txt.trim().equalsIgnoreCase("")){
        //         s.remove(txt);
        //     }
        // }
        // Iterator<String> iter = s.iterator();
        // while (iter.hasNext()) {
        //     String txt = iter.next();
        //     if (txt.trim().equalsIgnoreCase("")){
        //         s.remove(txt);
        //     }
        // }
        int len = s.size();
        for (int i = len - 1; i >= 0; i--) {
            if (s.get(i).trim().equalsIgnoreCase("")) {
                s.remove(i);
            }
        }
        System.out.println(s);
        System.out.println(s.get(0).length());

        String mm = "101";
        mm = String.format("%5s", mm).replace(" ","0");
        System.out.println(mm);
    }
}
