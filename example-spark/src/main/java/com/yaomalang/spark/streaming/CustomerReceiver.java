package com.yaomalang.spark.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class CustomerReceiver extends Receiver<String> {

    private String host;
    private int port;

    public CustomerReceiver(String host, int port) {
        this(StorageLevel.MEMORY_ONLY());
        this.host = host;
        this.port = port;
    }

    public CustomerReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    private void receive() {
        try {
            Socket socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String input = reader.readLine();
            while (!isStopped() && input != null) {
                store(input);
                input = reader.readLine();
            }
            reader.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStop() {
    }
}
