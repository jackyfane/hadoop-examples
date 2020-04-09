package com.yaomalang.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class CacheJoinDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {

        args = new String[]{"/users/yaomalang/input/order.txt", "/users/yaomalang/output_cachejoin"};

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        System.exit(ToolRunner.run(conf, new CacheJoinDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Cache Join Demo");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(getClass());
        job.setMapperClass(CacheJoinMapper.class);

        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        job.setCacheFiles(new URI[]{new URI("/users/yaomalang/input/product.txt")});

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
