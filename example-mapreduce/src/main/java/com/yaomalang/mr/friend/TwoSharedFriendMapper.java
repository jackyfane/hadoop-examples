package com.yaomalang.mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TwoSharedFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
        String friend = fields[0];
        String[] persons = fields[1].split(",");
        Arrays.sort(persons);
        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = 1; j < persons.length; j++) {
                k.set(persons[i] + "-" + persons[j]);
                v.set(friend);
                context.write(k, v);
            }
        }
    }
}