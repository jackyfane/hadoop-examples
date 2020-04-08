package com.yaomalang.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
//        config.set("fs.defaultFS.", "hdfs://192.168.56.101:9000");

        int code = ToolRunner.run(config, new WordCountDriver(), args);
        System.exit(code);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("args must be 2 length: input path and output path");
            return 1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("word count");

        job.setJarByClass(WordCountDriver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*CombineTextInputFormat从逻辑上对小文件进行合并，以降低因多个小文件而创建多个MapTask导致的性能低下问题，从而提高性能。*/
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
