package com.yaomalang.spark.sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public class AvgSalaryUDAF extends UserDefinedAggregateFunction {


    @Override
    public StructType inputSchema() {
        return new StructType(new StructField[]{new StructField("salary", DataTypes.LongType, false, Metadata.empty())});
    }

    @Override
    public StructType bufferSchema() {
        return new StructType(new StructField[]{
                new StructField("sum", DataTypes.LongType, true, Metadata.empty()),
                new StructField("count", DataTypes.LongType, true, Metadata.empty())
        });
    }

    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0);
        buffer.update(1, 0);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row row) {
        if (row != null) {
            buffer.update(0, buffer.getLong(0) + row.getLong(0));
            buffer.update(1, buffer.getInt(1) + 1);
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer) {
        buffer1.update(0, buffer.getLong(0) + buffer.getLong(0));
        buffer1.update(1, buffer.getInt(1) + buffer.getInt(1));
    }

    @Override
    public Object evaluate(Row row) {
        return row.getLong(0) / row.getInt(1);
    }
}
