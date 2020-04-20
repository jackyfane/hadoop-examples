package com.yaomalang.spark.core;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * example for kv
 */
public class APIExample03 {
    private JavaSparkContext sc;

    @Before
    public void init() {
        System.out.println("==================create SparkContext=================");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("API Example");
        sc = new JavaSparkContext(conf);
    }

    @After
    public void stop() {
        System.out.println("==================close SparkContext=================");
        sc.stop();
    }

    @Test
    public void partitionByTest() {
        List<Tuple2<Integer, String>> kvList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Tuple2 kv = new Tuple2(i + 1, "Value : " + i);
            kvList.add(kv);
        }
        JavaPairRDD javaPairRDD = sc.parallelizePairs(kvList);
        JavaPairRDD repartition = javaPairRDD.partitionBy(new HashPartitioner(2));
        System.out.println("Partition Number: ==========>" + repartition.getNumPartitions());
    }

    private List<Tuple2<String, Integer>> getPairData() {
        List<Tuple2<String, Integer>> kvList = new ArrayList<>();
        Tuple2 kv1 = new Tuple2("Hadoop", 200291);
        Tuple2 kv2 = new Tuple2("Spark", 2029382);
        Tuple2 kv3 = new Tuple2("Hadoop", 3203920);
        Tuple2 kv4 = new Tuple2("Hive", 200000);
        Tuple2 kv5 = new Tuple2("Spark", 342300);
        Tuple2 kv6 = new Tuple2("Hive", 820321);

        kvList.add(kv1);
        kvList.add(kv2);
        kvList.add(kv3);
        kvList.add(kv4);
        kvList.add(kv5);
        kvList.add(kv6);
        return kvList;
    }

    @Test
    public void reduceByKeyTest() {
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(getPairData());
        JavaPairRDD<String, Integer> reduceRDD = javaPairRDD.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println(reduceRDD.collect());
    }

    @Test
    public void groupByKeyTest() {
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(getPairData());
        JavaPairRDD<String, Iterable<Integer>> groupRDD = javaPairRDD.groupByKey();
        System.out.println("GroupByKey ============> " + groupRDD.collect());

//        JavaPairRDD<String, Integer> countRDD = groupRDD.mapValues(iter -> {
//            Iterator<Integer> iterator = iter.iterator();
//            int sum = 0;
//            while (iterator.hasNext()) {
//                sum += iterator.next();
//            }
//            return sum;
//        });

        JavaRDD<Tuple2> countRDD = groupRDD.map(v1 -> {
            String key = v1._1();
            Iterator<Integer> iter = v1._2.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next();
            }
            return new Tuple2(key, sum);
        });

//        [(Hive,1020321), (Spark,2371682), (Hadoop,3404211)]
        System.out.println("Map Values ===========> " + countRDD.collect());
    }

    //需求：获取每个分区中相同KEY的最大值，最后汇总求和
    @Test
    public void aggregateByKeyTest() {
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(getPairData(), 2);
        JavaPairRDD<String, Integer> aggregateRDD = javaPairRDD.aggregateByKey(0, (v1, v2) -> Math.max(v1, v2), (v1, v2) -> v1 + v2);
//        [(Hive,820321), (Spark,2371682), (Hadoop,3203920)]
        System.out.println(aggregateRDD.collect());
    }

    //    需求:创建一个 pairRDD，根据 key 计算每种 key 的均值。(先计算每个 key 出现的次数 以及可以对应值的总和，再相除得到结果)
    @Test
    public void combineByKey() {
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(getPairData(), 3);
        JavaPairRDD<String, Tuple2<Integer, Integer>> combineRDD = javaPairRDD.combineByKey(
                v -> new Tuple2(v, 1),
                (v1, v2) -> new Tuple2(v1._1 + v2, v1._2 + 1),
                (v1, v2) -> new Tuple2(v1._1 + v2._1, v1._2 + v2._2)
        );
        System.out.println("Combine RDD =====================> " + combineRDD.collect());

        JavaRDD<Tuple2<String, Float>> avgRDD = combineRDD.map(v -> new Tuple2(v._1, (float) (v._2._1 / v._2._2)));
        System.out.println("AVG Result =====================> " + avgRDD.collect());
    }

    @Test
    public void sortByKey() {
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(getPairData());
        JavaPairRDD<String, Integer> reduceRDD = javaPairRDD.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println(reduceRDD.sortByKey(false).collect());
    }

    @Test
    public void joinTest() {
        JavaPairRDD rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2(1, "a"), new Tuple2(2, "b"), new Tuple2(3, "c")));
        JavaPairRDD rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2(1, 4), new Tuple2(2, 5), new Tuple2(3, 6)));
        System.out.println(rdd1.join(rdd2).collect());
    }

    @Test
    public void cogroupTest() {
        JavaPairRDD rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2(1, "a"), new Tuple2(2, "b"), new Tuple2(3, "c")));
        JavaPairRDD rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2(1, 4), new Tuple2(2, 5), new Tuple2(3, 6)));
        System.out.println(rdd1.cogroup(rdd2).collect());
    }


}
