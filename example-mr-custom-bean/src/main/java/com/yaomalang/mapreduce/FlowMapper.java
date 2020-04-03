package com.yaomalang.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text key = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String[] fields = line != null ? line.toString().split(" ") : new String[]{};
        if (fields.length <= 0) return;
        key.set(fields[1]);

        flowBean.setUpFlow(Long.parseLong(fields[fields.length - 3]));
        flowBean.setDownFlow(Long.parseLong(fields[fields.length - 2]));

        context.write(key, flowBean);
    }
}
