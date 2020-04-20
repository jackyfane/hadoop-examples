package com.yaomalang.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

public class AvgSalaryAggregator extends Aggregator<Employee, Average, Float> {
    @Override
    public Average zero() {
        return new Average(0, 0);
    }

    @Override
    public Average reduce(Average average, Employee employee) {
        average.setSum(average.getSum() + employee.getSalary());
        average.setCount(average.getCount() + 1);
        return average;
    }

    @Override
    public Average merge(Average avg1, Average avg2) {
        Average average = new Average(avg1.getSum() + avg2.getSum(), avg1.getCount() + avg2.getCount());
        return average;
    }

    @Override
    public Float finish(Average average) {
        return average.getSum() / average.getCount();
    }

    @Override
    public Encoder<Average> bufferEncoder() {
//        return Encoders.product()
        return Encoders.bean(Average.class);
    }

    @Override
    public Encoder<Float> outputEncoder() {
        return Encoders.FLOAT();
    }
}
