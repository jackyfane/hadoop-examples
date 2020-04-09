package com.yaomalang.mr.join;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CacheJoinMapper extends Mapper<LongWritable, Text, Order, NullWritable> {

    private Map<String, String> prodMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI uri = context.getCacheFiles()[0];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(uri.getPath()));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis.getWrappedStream(), "UTF-8"));

        String line;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\t");
            prodMap.put(fields[0], fields[1]);
        }

        IOUtils.closeStream(fis);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        Order order = new Order(fields[0], fields[1], prodMap.get(fields[1]), Integer.parseInt(fields[2]), "");

        context.write(order, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        prodMap.clear();
    }
}
