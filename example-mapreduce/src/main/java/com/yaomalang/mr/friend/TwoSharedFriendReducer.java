package com.yaomalang.mr.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoSharedFriendReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        values.forEach(friend -> {
            sb.append(friend).append(" ");
        });
        context.write(key, new Text(sb.toString()));
    }
}