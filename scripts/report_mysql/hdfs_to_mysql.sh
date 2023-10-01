#!/bin/bash

DATAX_HOME=/opt/module/datax

#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path() {
  for i in $(hadoop fs -ls -R "$1" | awk '{print $8}'); do
    if hadoop fs -test -z "$i"; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f "$i"
    fi
  done
}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  handle_export_path "$export_dir"
  $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" "$datax_config"
}

case $1 in
'ads_mileage_stat_last_month' | 'ads_alarm_stat_last_month' | 'ads_temperature_stat_last_month' | 'ads_electric_stat_last_month' | 'ads_consume_stat_last_month')
  export_data "/opt/module/datax/job/export/car_data_report.$1.json" "/warehouse/car_data/ads/$1"
  ;;
'all')
  for table in 'ads_mileage_stat_last_month' 'ads_alarm_stat_last_month' 'ads_temperature_stat_last_month' 'ads_electric_stat_last_month' 'ads_consume_stat_last_month'; do
    export_data "/opt/module/datax/job/export/car_data_report.$table.json" "/warehouse/car_data/ads/$table"
  done
  ;;
esac
