package com.yaomalang.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneSharedFriendReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text person : values) {
            sb.append(person).append(",");
        }
        context.write(key, new Text(sb.toString()));
    }
}