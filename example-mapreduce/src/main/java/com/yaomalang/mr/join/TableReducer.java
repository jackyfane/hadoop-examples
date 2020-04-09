package com.yaomalang.mr.join;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, Order, Order, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Order> values, Context context) throws IOException, InterruptedException {
//        1001	01	1
//        01	小米

        List<Order> orders = new ArrayList<>();

        Order prod = new Order();

        for (Order order : values) {
            if ("product".equals(order.getLabel())) {
                prod.setProductId(key.toString());
                prod.setProductName(order.getProductName());
            } else {
                Order tmp = new Order();
                tmp.setOrderId(order.getOrderId());
                tmp.setProductId(order.getProductId());
                tmp.setAmount(order.getAmount());
                tmp.setLabel(order.getLabel());

                orders.add(tmp);
            }
        }

        for (Order order : orders) {
            order.setProductName(prod.getProductName());
            context.write(order, NullWritable.get());
        }
    }
}
