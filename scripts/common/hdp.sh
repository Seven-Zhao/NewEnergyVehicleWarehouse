#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="

        echo " --------------- 启动 hdfs ---------------"
        ssh hadoop101 "/opt/module/hadoop-3.3.6/sbin/start-dfs.sh"
        echo " --------------- 启动 yarn ---------------"
        ssh hadoop101 "/opt/module/hadoop-3.3.6/sbin/start-yarn.sh"
        echo " --------------- 启动 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.3.6/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="

        echo " --------------- 关闭 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.3.6/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 yarn ---------------"
        ssh hadoop101 "/opt/module/hadoop-3.3.6/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop101 "/opt/module/hadoop-3.3.6/sbin/stop-dfs.sh"
;;
*)
    echo "Input Args Error..."
;;
esac
