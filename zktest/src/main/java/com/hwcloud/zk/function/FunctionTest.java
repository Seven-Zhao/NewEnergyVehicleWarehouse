package com.hwcloud.zk.function;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * ClassName:    FunctionTest
 * Package:      com.hwcloud.zk.function
 *
 * @Author: Seven
 * @Create: 2023/9/17 21:22
 * @Version:
 * @Description:
 */
public class FunctionTest {
    private static final String ZOOKEEPER_CONNECT_STRING = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private static final int TIMEOUT = 2000;
    private ZooKeeper zkClient = null;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(ZOOKEEPER_CONNECT_STRING, TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getType() + "--" + event.getPath());

                try {
                    zkClient.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 创建子节点测试
     */
    @Test
    public void create() throws InterruptedException, KeeperException {
        // 检查节点是否存在
        Stat stat = zkClient.exists("/test", false);

        if (stat == null) {
            //参数：要创建节点的路径，节点数据，节点权限，节点类型
            String createdNode = zkClient.create(
                    "/test",
                    "Seven".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

        } else {
            System.out.println("/test node already exists.");
        }
    }

    /**
     * 获取子节点并监听节点变化
     * 需要在创建Zookeeper的客户端时，设置Watcher，即编写process回调方法。
     */
    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        try (ZooKeeper client = new ZooKeeper(ZOOKEEPER_CONNECT_STRING, TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        })) {
            // 在 try 代码块中使用 ZooKeeper 客户端进行操作
            List<String> children = client.getChildren("/", true);

            for (String child : children) {
                System.out.println(child);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
