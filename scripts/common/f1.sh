#!/bin/bash

case $1 in
"start") {
  echo " --------启动hadoop102采集flume-------"
  ssh hadoop101 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf/ -f /opt/module/flume/job/file_to_kafka.conf >/dev/null 2>&1 &"
} ;;
"stop") {
  echo " --------停止hadoop102采集flume-------"
  ssh hadoop101 "ps -ef | grep file_to_kafka | grep -v grep |awk  '{print \$2}' | xargs -n1 kill -9 "
} ;;
esac
