#!/bin/bash

APP='car_data'

if [ -n "$2" ]; then
  do_date=$2
else
  do_date=$(date -d '-1 day' +%F)
fi

dim_car_info_full="
insert overwrite table ${APP}.dim_car_info_full partition (dt = '$do_date')
select id,
       type_id,
       type,
       sale_type,
       trademark,
       company,
       seating_capacity,
       power_type,
       charge_type,
       category,
       weight_kg,
       warranty
from ${APP}.ods_car_info_full o
where o.dt = '$do_date';
"

case $1 in
'dim_car_info_full')
  hive -e "${dim_car_info_full}"
  ;;
"all")
  hive -e "${dim_car_info_full}"
  ;;
esac
