package com.yaomalang.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return;
        boolean isValid = parseLog(value.toString());
        if (!isValid) return;

        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line) {
        String[] fields = line.split("\\s+");
        if (fields.length == 15)
            return true;
        return false;
    }
}
