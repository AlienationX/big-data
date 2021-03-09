package com.ego.examples;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CaseThread {

    public static void main(String[] args) {
        ExecutorService es = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 6; i++) {
            es.submit(new Task("" + i));
        }
        es.shutdown();
    }
}


class Task implements Runnable {

    private final String name;

    Task(String name) {
        this.name = name;
    }


    @Override
    public void run() {
        try {
            int num = new Random().nextInt(4) + 1;
            System.out.println(LocalDateTime.now() + ": start task " + this.name + ", " + num + "s");
            Thread.sleep(num * 1000);
            System.out.println(LocalDateTime.now() + ": end task " + this.name);
        } catch (InterruptedException e) {
            e.getStackTrace();
        }
    }
}
