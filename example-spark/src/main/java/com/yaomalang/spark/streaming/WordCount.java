package com.yaomalang.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(3000));
//        JavaReceiverInputDStream<String> lienStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY());
        JavaReceiverInputDStream<String> lienStream = ssc.receiverStream(new CustomerReceiver("localhost", 9999));

        JavaDStream<String> wordStreams = lienStream.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairStreams = wordStreams.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairDStream<String, Integer> wordCountStream = pairStreams.reduceByKey(Integer::sum);

        wordCountStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
