package com.yaomalang.mr.join;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements WritableComparable<Order> {

    private String orderId;
    private String productId;
    private String productName;
    private int amount;

    public Order() {
    }

    public Order(String orderId, String productId, String productName, int amount) {
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
        this.amount = amount;
    }

    @Override
    public int compareTo(Order order) {
        if (orderId.hashCode() > order.getOrderId().hashCode()) {
            return 1;
        } else if (orderId.hashCode() < order.getOrderId().hashCode()) {
            return -1;
        } else {
            if (this.amount > order.getAmount()) {
                return 1;
            } else if (this.amount < order.getAmount()) {
                return -1;
            }
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(productName);
        dataOutput.writeInt(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        productId = dataInput.readUTF();
        productName = dataInput.readUTF();
        amount = dataInput.readInt();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return orderId + "\t" + productName + "\t" + "\t" + amount;
    }
}
