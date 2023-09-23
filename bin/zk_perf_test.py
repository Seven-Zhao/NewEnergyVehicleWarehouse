#!/usr/bin/python
# _*_ coding: utf-8 _*_
# @Time    : 2023/9/17 11:09
# @Author  : Seven
# @File    : zk_perf_test.py
import threading
import time
from kazoo.client import KazooClient

# 定义测试参数
num_threads = 10  # 并发线程数
num_requests = 1000  # 每个线程发送的请求次数
zk_hosts = 'hadoop101:2181'  # ZooKeeper服务器地址

# 初始化计数器
counter = 0


# 定义测试线程
class TestThread(threading.Thread):
    def run(self):
        global counter
        zk = KazooClient(hosts=zk_hosts)  # 创建ZooKeeper客户端连接
        zk.start()
        for _ in range(num_requests):
            # 在这里执行具体的ZooKeeper操作
            # 例如：创建节点、读取节点数据等
            zk.create('/test_node')
            counter += 1
        zk.stop()


if __name__ == '__main__':
    # 创建并启动测试线程
    threads = []
    start_time = time.time()
    for _ in range(num_threads):
        thread = TestThread()
        threads.append(thread)
        thread.start()

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    # 计算吞吐量
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = counter / elapsed_time

    # 输出结果
    print(f'Total requests: {counter}')
    print(f'Total time: {elapsed_time:.2f} seconds')
    print(f'Throughput: {throughput:.2f} requests per second')
