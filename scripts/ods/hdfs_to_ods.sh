#!/bin/bash

APP='car_data'

# 判断第二个参数是否填写  如果填写使用作为日期 如果没有填写 默认使用昨天作为日期
if [ -n "$2" ]; then
  do_date=$2
else
  do_date=$(date -d '-1 day' +%F)
fi

case "$1" in
"ods_car_data_inc")
  hive -e "load data inpath '/origin_data/car_data_full/$do_date' into table $APP.ods_car_data_inc partition (dt='$do_date');"
  ;;
"ods_car_info_full")
  hive -e "load data inpath '/origin_data/car_info_full/$do_date' into table $APP.ods_car_info_full partition (dt='$do_date');"
  ;;
"all")
  hive -e "load data inpath '/origin_data/car_data_full/$do_date' into table $APP.ods_car_data_inc partition (dt='$do_date');
	load data inpath '/origin_data/car_info_full/$do_date' into table  $APP.ods_car_info_full partition (dt='$do_date');"
  ;;
esac
