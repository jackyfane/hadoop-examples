package com.yaomalang.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class DataFrameExample {

    private SparkSession spark;
    private SparkContext sc;

    @Before
    public void init() {
        System.out.println("================start==================");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SQLRead");
        sc = new SparkContext(conf);
        spark = new SparkSession(sc);
//        spark = SparkSession.builder().appName("").master("local[*]").getOrCreate();
        spark.implicits();
    }

    @After
    public void stop() {
        System.out.println("================stop==================");
        if (spark != null) spark.stop();
        if (sc != null) sc.stop();
    }

    @Test
    public void readJSON() throws AnalysisException {
        Dataset<Row> df = spark.read().json("/Users/yaomalang/Repositories/BigDataDemos/hadoop-examples/dataset/people.json");
        df.createTempView("people");
//        df.createOrReplaceTempView("people");
        spark.sql("select * from people").show();
//        df.createGlobalTempView("people");
//        spark.sql("select * from global_temp.people").show();

//        DSL风格
        df.select("name").show();
    }

    @Test
    public void rddToDataFrame1() throws AnalysisException {
        JavaRDD<String> rdd = sc.textFile("/Users/yaomalang/Repositories/BigDataDemos/hadoop-examples/dataset/people.txt", 1)
                .toJavaRDD();
        JavaRDD<People> peopleRDD = rdd.map(s -> {
            String[] fields = s.split(",");
            return new People(fields[0], "".equals(fields[1]) ? Integer.valueOf(fields[1]) : null);
        });
        Dataset<Row> df = spark.createDataFrame(peopleRDD, People.class);
        df.show();
    }

    @Test
    public void rddToDataFrame2() throws AnalysisException {
        JavaRDD<String> rdd = sc.textFile("/Users/yaomalang/Repositories/BigDataDemos/hadoop-examples/dataset/people.txt", 1)
                .toJavaRDD();
        JavaRDD<Row> rowJavaRDD = rdd.map(s -> {
            String[] fields = s.split(",");
            return RowFactory.create(fields[0], "".equals(fields[1]) ? Integer.valueOf(fields[1]) : null);
        });
        ArrayList<StructField> fields = new ArrayList<>();
        StructField field = null;
        field = DataTypes.createStructField("name", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = spark.createDataFrame(rowJavaRDD, schema);
        df.select("name", "age").show();
    }

    @Test
    public void dataFrameToRDD() {
        Dataset<Row> df = spark.read().json("/Users/yaomalang/Repositories/BigDataDemos/hadoop-examples/dataset/people.json");
        JavaRDD<Row> rowJavaRDD = df.javaRDD();
        spark.udf().register("addName", new AvgSalaryUDAF());
        System.out.println(rowJavaRDD.collect());
    }
}
