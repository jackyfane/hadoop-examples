package com.yaomalang.mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OneSharedFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(":");
        String person = fields[0];
        String[] friends = fields[1].split(",");
        for (int i = 0; i < friends.length; i++) {
            k.set(friends[i]);
            v.set(person);
            context.write(k, v);
        }
    }
}