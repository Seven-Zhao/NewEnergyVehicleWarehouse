#!/bin/bash
if [ "$#" -lt 1 ]; then
  echo "没有传入日期参数！"
  exit 1
fi

if [ -n "$2" ]; then
  username=$2
else
  username=root
fi

if [ -n "$3" ]; then
  passwd=$3
else
  passwd='000000'
fi

java -jar \
  /opt/module/car-data/car-data-1.0.1-jar-with-dependencies.jar \
  -c 1 \
  -o /opt/module/car-data/data \
  -d "$1" \
  -n $username \
  -p $passwd \
  -u jdbc:mysql://hadoop101:3306/car_data
