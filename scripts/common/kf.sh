#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
  exit
fi

if [[ "$1" == "-h="* ]]; then
  #Parse hosts from argument
  hosts=$(echo "${1#*=}" | tr ',' '\n')
  shift
else
  hosts=$(cat ~/bin/host_ips)
fi

case $1 in
start)
  for i in $hosts; do
    echo "================> START $i KF <=================="
    ssh "$i" kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties
  done
  ;;

stop)
  for i in $hosts; do
    echo "=================> STOP $i KF <=================="
    ssh "$i" kafka-server-stop.sh
  done
  ;;

kc)
  if [ "$2" ]; then
    kafka-console-consumer.sh --bootstrap-server hadoop101:9092,hadoop102:9092,hadoop103:9092 --topic "$2"
  else
    echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list|delete [topic] |describe [topic]}"
  fi
  ;;

kp)
  if [ "$2" ]; then
    kafka-console-producer.sh --broker-list hadoop101:9092,hadoop102:9092,hadoop103:9092 --topic "$2"
  else
    echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list|delete [topic] |describe [topic]}"
  fi
  ;;

list)
  kafka-topics.sh --list --bootstrap-server hadoop101:9092,hadoop102:9092,hadoop103:9092
  ;;

create)
  # shellcheck disable=SC2074
  # shellcheck disable=SC2107
  if [ -n "$2" ] && [ "$3" =~ ^[0-9]+$ && "$4" =~ ^[0-9]+$ ]; then
    kafka-topics.sh --bootstrap-server hadoop102:9092 --create --partitions "$3" --replication-factor "$4" --topic "$2"
  elif [ -n "$2" ] && [ "$3" =~ ^[0-9]+$ ]; then
    kafka-topics.sh --bootstrap-server hadoop102:9092 --create --partitions "$3" --topic "$2"
  elif [ -n "$2" ]; then
    kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic "$2"
  else
    echo "Invalid parameters, Usage:kf.sh create topic part_num replica_num"
  fi
  ;;

describe)
  if [ "$2" ]; then
    kafka-topics.sh --describe --bootstrap-server hadoop101:9092,hadoop102:9092,hadoop103:9092 --topic "$2"
  else
    echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list|delete [topic] |describe [topic]}"
  fi
  ;;

delete)
  if [ "$2" ]; then
    kafka-topics.sh --delete --bootstrap-server hadoop102:9092,hadoop103:9092,hadoop104:9092 --topic "$2"
  else
    echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list|delete [topic] |describe [topic]}"
  fi
  ;;

*)
  echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete[topic] |describe [topic]}"
  exit
  ;;
esac
