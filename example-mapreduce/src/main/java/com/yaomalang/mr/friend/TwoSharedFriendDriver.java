package com.yaomalang.mr.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwoSharedFriendDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        args = new String[] { "/users/yaomalang/output_friends_one", "/users/yaomalang/output_friends_two" };

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        System.exit(ToolRunner.run(conf, new TwoSharedFriendDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Two Shared Friends Demo");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(getClass());
        job.setMapperClass(TwoSharedFriendMapper.class);
        job.setReducerClass(TwoSharedFriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}