#!/bin/bash
if [ "$#" -lt 1 ]; then
  echo "参数错误！"
  exit 1
fi

targetdir="/origin_data/car_info_full/$1"
# 确保HDFS目录存在且为空
hadoop fs -rm -r "${targetdir}"
hadoop fs -mkdir -p "${targetdir}"

# 执行DataX任务
/opt/module/datax/bin/datax.py /opt/module/datax/job/car_info.json -p"-Dtargetdir=${targetdir}"
