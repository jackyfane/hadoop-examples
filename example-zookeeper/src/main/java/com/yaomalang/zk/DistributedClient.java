package com.yaomalang.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedClient {
    private final String connectString = "192.168.56.101:2181,192.168.56.102:2181,192.168.56.103:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zkClient;
    private final String parentNode = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributedClient client = new DistributedClient();
        client.getConnect();
        client.getServerList();
        client.business();
    }

    public void business() throws InterruptedException {
        System.out.println("client is working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
            try {
                getServerList();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public void getServerList() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(parentNode, true);
        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        System.out.println(servers);
    }
}
