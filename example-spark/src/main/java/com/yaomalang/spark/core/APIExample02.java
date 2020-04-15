package com.yaomalang.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * example for double value
 */
public class APIExample02 {
    private JavaSparkContext sc;

    @Before
    public void init() {
        System.out.println("==================创建SparkContext=================");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("API Example");
        sc = new JavaSparkContext(conf);
    }

    @After
    public void clean() {
        System.out.println("==================关闭SparkContext=================");
        sc.stop();
    }

    @Test
    public void unionTest() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(6, 7, 8, 9, 10));
        JavaRDD<Integer> rdd3 = rdd1.union(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void subtractTest() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(5, 4, 8, 9, 10));
        JavaRDD<Integer> rdd3 = rdd1.subtract(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void intersectionTest() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(5, 4, 8, 9, 10));
        JavaRDD<Integer> rdd3 = rdd1.intersection(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void cartesianTest() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(8, 9, 10));
        JavaPairRDD<Integer, Integer> rdd3 = rdd1.cartesian(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void zipTest() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3), 3);
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c"), 3);
        JavaPairRDD<Integer, String> rdd3 = rdd1.zip(rdd2);
        System.out.println(rdd3.collect());
    }

}
