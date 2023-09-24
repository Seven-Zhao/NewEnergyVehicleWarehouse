#!/bin/bash

case $1 in
"start")
echo " --------启动hadoop103日志数据flume-------"
ssh hadoop103 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/kafka_to_hdfs.conf >/dev/null 2>&1 &"
;;
"stop")
echo " --------停止hadoop103日志数据flume-------"
ssh hadoop103 "ps -ef | grep kafka_to_hdfs | grep -v grep |awk '{print \$2}' | xargs -n1 kill"
;;
esac