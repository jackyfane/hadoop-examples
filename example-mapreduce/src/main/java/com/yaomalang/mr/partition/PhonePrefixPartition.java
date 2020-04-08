package com.yaomalang.mr.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePrefixPartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        int partitionNo;
        String phone = key.toString();
        if (phone.startsWith("135")) {
            partitionNo = 1;
        } else if (phone.startsWith("136")) {
            partitionNo = 2;
        } else if (phone.startsWith("137")) {
            partitionNo = 3;
        } else if (phone.startsWith("138")) {
            partitionNo = 4;
        } else {
            partitionNo = 0;
        }
        return partitionNo;
    }
}
