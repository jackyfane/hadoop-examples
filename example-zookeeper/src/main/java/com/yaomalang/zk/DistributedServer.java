package com.yaomalang.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class DistributedServer {

    private final String connectString = "192.168.56.101:2181,192.168.56.102:2181,192.168.56.103:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zkClient;
    private final String parentNode = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributedServer server = new DistributedServer();
        server.connect();
        server.register(args[0]);
        server.business(args[0]);
    }

    /**
     * business deal
     *
     * @param hostname
     * @throws InterruptedException
     */
    private void business(String hostname) throws InterruptedException {
        System.out.println("{0} is working....".format(hostname));
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * register server
     *
     * @param hostname
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void register(String hostname) throws KeeperException, InterruptedException {
        if (zkClient.exists(parentNode + "/server", false) != null)
            return;
        String created = zkClient.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("{0} is online {1}".format(hostname, created));
    }

    /**
     * Connect Zookeeper Server
     *
     * @throws IOException
     */
    private void connect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
        });
    }


}
