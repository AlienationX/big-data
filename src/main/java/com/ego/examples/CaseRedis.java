package com.ego.examples;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CaseRedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop-dev04", 6379);
        jedis.set("name", "I am Redis");
        jedis.set("tag", "nosql");
        System.out.println(jedis.get("name"));

        //存储数据到列表中
        jedis.lpush("site-list", "Microsoft");
        jedis.lpush("site-list", "Google");
        jedis.lpush("site-list", "Apple");
        // 获取存储的数据并输出
        List<String> list = jedis.lrange("site-list", 0, 2);
        for (int i = 0; i < list.size(); i++) {
            System.out.println("列表项为: " + list.get(i));
        }

        Set<String> keys = jedis.keys("*");
        Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String k = it.next();
            if (k.equals("site-list")) {
                continue;
            }
            String v = jedis.get(k);
            System.out.println(k + ":" + v);
        }
    }
}
