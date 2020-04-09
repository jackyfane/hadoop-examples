package com.yaomalang.mr.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, Order> {

    private String label;
    private Order order = new Order();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.label = ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        1001	01	1
//        01	小米
        String[] fields = value.toString().split("\t");
        if (label.startsWith("product")) {
            order.setOrderId("");
            order.setProductId(fields[0]);
            order.setProductName(fields[1]);
            order.setAmount(0);
            order.setLabel("product");

            k.set(fields[0]);
        } else if (label.startsWith("order")) {
            order.setOrderId(fields[0]);
            order.setProductId(fields[1]);
            order.setProductName("");
            order.setAmount(Integer.parseInt(fields[2]));
            order.setLabel("order");

            k.set(fields[1]);
        }

        context.write(k, order);
    }
}
