package com.yaomalang.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * example for single value
 */
public class APIExample01 {

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
    public void mapTest() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        JavaRDD<Integer> mapRDD = rdd.map(x -> x * 2);
        List<Integer> results = mapRDD.collect();
        System.out.println(results);
    }

    @Test
    public void mapPartitionsTest() {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 4);
        JavaRDD<Integer> mapPartitionsRDD = javaRDD.mapPartitions(iterator -> {
            List<Integer> outer = new LinkedList<>();
            while (iterator.hasNext())
                outer.add(iterator.next() * 2);
            return outer.iterator();
        });
        System.out.println(mapPartitionsRDD.collect());
    }

    @Test
    public void mapPartitionsWithIndexTest() {
        JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("张三", "李四", "王二", "麻子"), 2);
        JavaRDD<String> parseRDD = javaRDD.mapPartitionsWithIndex((index, iter) -> {
            ArrayList<String> results = new ArrayList<>();
            while (iter.hasNext()) {
                results.add(String.format("第%s分区的数据:%s", index, iter.next()));
            }
            return results.iterator();
        }, false);
        System.out.println(parseRDD.collect());
    }

    @Test
    public void flatMapTest() {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> results = javaRDD.flatMap(integer -> {
            ArrayList<Integer> arrayList = new ArrayList();
            for (int i = 0; i < integer.intValue(); i++) {
                arrayList.add(i + 1);
            }
            return arrayList.iterator();
        });
        System.out.println(results.collect());
    }

    @Test
    public void glomTest() {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16), 4);
        JavaRDD<List<Integer>> glom = javaRDD.glom();
        System.out.println(glom.collect());
    }

    @Test
    public void groupByTest() {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaPairRDD<Integer, Iterable<Integer>> group = javaRDD.groupBy(x -> x % 2);
        System.out.println(group.collect());
    }

    @Test
    public void filterTest() {
        JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("张三", "李四", "王二", "麻子", "老二", "老三", "老四"));
        JavaRDD<String> results = javaRDD.filter(x -> x.contains("老"));
        System.out.println(results.collect());
    }

    @Test
    public void sampleTest() {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
//        JavaRDD<Integer> sample = javaRDD.sample(false, 0.4, 2);
        JavaRDD<Integer> sample = javaRDD.sample(true, 0.4, 2);
        System.out.println(sample.collect());
    }

    @Test
    public void coalesceTest() {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(8, 9, 10, 11, 1, 2, 3, 14, 15, 4, 5, 6, 7, 12, 13, 16), 4);
        JavaRDD<Integer> coalesce = javaRDD.coalesce(3);
        System.out.println(coalesce.getNumPartitions());
    }

    @Test
    public void pipeTest() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Java", "Python", "GoLang", "Dart"), 2);
        JavaRDD<String> pipe = rdd.pipe("src/main/resources/pipe.sh");
        System.out.println(pipe.collect());
    }


}
