package com.yaomalang.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * args[0]: file path
 * args[1]: delimiter
 * args[2]: hbase table name
 * args[3+]: column family,format separate by:,example "info:name"
 */
public class ImportTSVDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(ImportTSVDriver.class);
        job.setJobName("ImportTSV");

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(ImportTSVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(args[2], ImportTSVReducer.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = createConfiguration(args);
        int run = ToolRunner.run(configuration, new ImportTSVDriver(), args);
        System.exit(run);
    }

    /**
     * @param args
     * @return
     */
    private static Configuration createConfiguration(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("fs.defaultFS", "hdfs://node001:9000");
        configuration.set("hbase.zookeeper.quorum", "node001");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("delimiter", args[1]);

        String[] families = new String[args.length - 3];
        for (int i = 0; i < families.length; i++) {
            families[i] = args[i+3];
        }
        configuration.setStrings("column.families", families);

        return configuration;
    }
}
