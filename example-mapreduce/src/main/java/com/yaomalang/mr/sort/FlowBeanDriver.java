package com.yaomalang.mr.sort;

import com.yaomalang.mr.partition.PartitionDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowBeanDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        args = new String[]{"/users/yaomalang/input/phone_data.txt", "/users/yaomalang/output_sort"};

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        System.exit(ToolRunner.run(conf, new PartitionDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setJobName("Sort Demo");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
