package com.midway.utils;

import org.apache.spark.api.java.JavaRDDLike;

public class P {
    public static void println(String s) {
        System.out.println(s);
    }

    public static void println(Object o) {
        System.out.println(o.toString());
    }

    public static <T extends JavaRDDLike> void println(T rdd) {
        rdd.foreach(e -> println(e));
    }

    public static void printlnn(Object o) {
        println(o);
        println("");
    }

    public static <T extends JavaRDDLike> void  printlnn(T rdd) {
        println(rdd);
        println("");
    }
}
