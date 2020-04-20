package com.yaomalang.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AdTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("AdTop3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> javaRDD = sc.textFile("/Users/yaomalang/Repositories/BigDataDemos/hadoop-examples/dataset/agent.log");

        JavaPairRDD<Tuple2, Integer> provinceAdToOne = javaRDD.map(line -> {
            String[] fields = line.split(" ");
            return new Tuple2(new Tuple2(fields[1], fields[3]), 1);
        }).mapToPair(v -> v);

        JavaPairRDD<Tuple2, Integer> provinceAdToSum = provinceAdToOne.reduceByKey((v1, v2) -> v1 + v2);

        JavaPairRDD<String, Tuple2<String, Integer>> provinceToAdSum = provinceAdToSum.map(x -> new Tuple2(x._1._1, new Tuple2<>(x._1._2, x._2))).mapToPair(v -> v);
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> provinceToAdSumGroup = provinceToAdSum.groupByKey();
        JavaPairRDD<String, List<Tuple2<String, Integer>>> provinceAdSumTop3 = provinceToAdSumGroup.mapValues(iter -> {
            Iterator<Tuple2<String, Integer>> iterator = iter.iterator();
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            list.sort((x, y) -> x._2 > y._2 ? -1 : (x._2 == y._2 ? 0 : 1));
            if (list.size() <= 3)
                return list;
            return list.subList(0, 3);
        });

        System.out.println("Province Ad Sum Top 3 = " + provinceAdSumTop3.collect());

    }
}
