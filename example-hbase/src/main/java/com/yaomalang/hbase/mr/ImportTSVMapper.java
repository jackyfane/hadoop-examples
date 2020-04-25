package com.yaomalang.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImportTSVMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

    private String delimiter;
    private String[] families;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        delimiter = config.get("delimiter", ",");
        families = config.getStrings("column.families");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(delimiter);

        rowKey.set(Bytes.toBytes(fields[0]));

        Put put = new Put(Bytes.toBytes(fields[0]));
        for (int i = 0; i < families.length; i++) {
            String[] cf = families[i].split(":");
            put.addColumn(Bytes.toBytes(cf[0]), Bytes.toBytes(cf[1]), Bytes.toBytes(fields[i + 1]));
        }

        context.write(rowKey, put);
    }
}
