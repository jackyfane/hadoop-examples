package com.yaomalang.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
//        config.set("fs.defaultFS.", "hdfs://192.168.56.101:9000");

        int code = ToolRunner.run(config, new WordCount(), args);
        System.exit(code);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("args must be 2 length: input path and output path");
            return 1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("word count");

        job.setJarByClass(WordCount.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }
}
