package com.yaomalang.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZKTest {

    private final String connectString = "192.168.56.101:2181,192.168.56.102:2181,192.168.56.103:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, (event) -> {
            System.out.println(event.getType() + " -- " + event.getPath());
            try {
                zkClient.getChildren("/", true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void watch() throws InterruptedException, KeeperException {

        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/yaomalang/zhangsan", "guy".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("created path : " + path);
    }

    @Test
    public void getValue() throws KeeperException, InterruptedException {
        byte[] bytes = zkClient.getData("/yaomalang/zhangsan", false, null);
        System.out.println("/yaomalang/zhangsan'data is :" + new String(bytes));
    }

    @Test
    public void setValue() throws KeeperException, InterruptedException {
        zkClient.setData("/yaomalang/zhangsan", "ohmygod".getBytes(), 0);
        getValue();
    }

    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        zkClient.delete("/yaomalang/zhangsan", 1);
    }
}
