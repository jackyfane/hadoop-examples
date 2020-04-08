package com.yaomalang.mr.kvformat;

import com.yaomalang.mr.wordcount.WordCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * KeyValueTextInputFormat Demo
 * KeyValueTextInputFormat的格式是将一行数据进行分割为两列的一维数组，其中第一列将作为Map的key，第二列为Map的value
 * 默认的分割是制表符：\t，可以通过Configuration设置KeyValueLineRecordReader.KEY_VALUE_SEPERATOR分隔符，
 * 如空格符的设置，config.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ")，
 * 同时，需要在Job中设置输入格式class为KeyValueTextFileFormat：
 * job.setInputFormatClass(KeyValueTextInputFormat.class);
 */
public class KVTextDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        config.set("fs.defaultFS", "hdfs://localhost:9000");

        int code = ToolRunner.run(config, new KVTextDriver(), args);
        System.exit(code);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("args must be 2 length: input path and output path");
            return 1;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("KV Text Demo");

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setJarByClass(KVTextDriver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
