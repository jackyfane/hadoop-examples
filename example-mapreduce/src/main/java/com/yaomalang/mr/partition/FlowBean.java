package com.yaomalang.mr.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private long upFlow = 0;
    private long downFlow = 0;
    private long sumFlow = 0;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(upFlow);
        output.writeLong(downFlow);
        output.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        upFlow = input.readLong();
        downFlow = input.readLong();
        sumFlow = input.readLong();
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}