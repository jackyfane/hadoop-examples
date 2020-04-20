package com.yaomalang.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("/Users/yaomalang/Documents/hadoop/input/text1.txt", 1);
        JavaRDD<String> words = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordOne = words.mapToPair(w -> new Tuple2(w, 1));
        JavaPairRDD<String, Integer> wordCounter = wordOne.reduceByKey((a, b) -> a + b);

        wordCounter.saveAsTextFile("/Users/yaomalang/Documents/hadoop/output");
    }
}
