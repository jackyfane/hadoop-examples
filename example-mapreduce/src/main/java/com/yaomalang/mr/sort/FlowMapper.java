package com.yaomalang.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text v = new Text();

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String[] fields = line.toString().split(" ");
        if (fields.length <= 0) return;
        v.set(fields[0]);

        FlowBean key = new FlowBean(Long.parseLong(fields[1]), Long.parseLong(fields[2]));

        context.write(key, v);
    }
}
