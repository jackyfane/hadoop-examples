package com.yaomalang.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text key = new Text();
    private IntWritable oValue = new IntWritable(1);

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
        String valueStr = value != null ? value.toString() : "";
        String[] words = valueStr.split(" ");
        for (String word : words) {
            word = word.replace(",", "");
            key.set(word);
            context.write(key, oValue);
        }
    }
}
