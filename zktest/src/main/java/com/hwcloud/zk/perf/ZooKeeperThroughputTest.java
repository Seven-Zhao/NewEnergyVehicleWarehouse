package com.hwcloud.zk.perf;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * ClassName:    ZooKeeperThroughputTest
 * Package:      com.hwcloud.zk.perf
 *
 * @Author: Seven
 * @Create: 2023/9/17 21:11
 * @Version:
 * @Description:
 */
public class ZooKeeperThroughputTest {
    private static final int NUM_CLIENTS = 100; // 并发客户端数量
    private static final int NUM_REQUESTS = 1000; // 每个客户端发送的请求数量
    private static final String ZOOKEEPER_CONNECT_STRING = "hadoop101:2181"; // ZooKeeper连接字符串

    public static void main(String[] args) throws Exception {
        CountDownLatch latch = new CountDownLatch(NUM_CLIENTS);
        for (int i = 0; i < NUM_CLIENTS; i++) {
            new Thread(() -> {
                try {
                    ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_CONNECT_STRING, 5000, null);

                    for (int j = 0; j < NUM_REQUESTS; j++) {
                        // 发送请求并处理响应
                        String path = "/test" + j;
                        byte[] data = "testdata".getBytes();
                        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        Stat stat = new Stat();
                        zooKeeper.getData(path, false, stat);
                        zooKeeper.delete(path, stat.getVersion());
                    }

                    zooKeeper.close();
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }

        latch.await(); // 等待所有线程执行完毕
    }
}
