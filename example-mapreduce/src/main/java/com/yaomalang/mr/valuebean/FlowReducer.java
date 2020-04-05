package com.yaomalang.mr.valuebean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowSum = 0;
        long downFlowSum = 0;

        for (FlowBean flowBean : values) {
            upFlowSum = flowBean.getUpFlow();
            downFlowSum = flowBean.getDownFlow();
        }

        context.write(key, new FlowBean(upFlowSum, downFlowSum));
    }
}
