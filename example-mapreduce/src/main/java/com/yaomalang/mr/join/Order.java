package com.yaomalang.mr.join;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements Writable {

    private String orderId;
    private String productId;
    private String productName;
    private int amount;
    private String label;

    public Order() {
    }

    public Order(String orderId, String productId, String productName, int amount, String label) {
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
        this.amount = amount;
        this.label = label;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(productName);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(label);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        productId = dataInput.readUTF();
        productName = dataInput.readUTF();
        amount = dataInput.readInt();
        label = dataInput.readUTF();
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

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return orderId + "\t" + productName + "\t" + "\t" + amount;
    }
}
