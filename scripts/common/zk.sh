#!/bin/bash

hosts=$(cat ~/bin/host_ips)

case $1 in
"start") {
  for host in $hosts; do
    echo "-------- start zookeeper $host --------"
    ssh "$host" "/opt/module/zookeeper-3.9/bin/zkServer.sh start"
  done
} ;;

"stop") {
  for host in $hosts; do
    echo "-------- stop zookeeper $host --------"
    ssh "$host" "/opt/module/zookeeper-3.9/bin/zkServer.sh stop"
  done
} ;;

"status") {
  for host in $hosts; do
    echo "-------- stop zookeeper $host --------"
    ssh "$host" "/opt/module/zookeeper-3.9/bin/zkServer.sh status"
  done
} ;;
esac
