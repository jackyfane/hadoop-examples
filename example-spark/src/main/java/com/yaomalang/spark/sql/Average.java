package com.yaomalang.spark.sql;

public class Average {
    private float sum;
    private int count;

    public Average() {
    }

    public Average(float sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public float getSum() {
        return sum;
    }

    public void setSum(float sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
