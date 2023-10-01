#!/bin/bash

APP='car_data'

if [ -n "$2" ]; then
  do_date=$2
else
  do_date=$(date -d '-1 day' +%F)
fi

dws_car_mileage_1d="
insert into ${APP}.dws_car_mileage_1d partition(dt)
select vin,
       max(mileage) - min(mileage),
       sum(velocity),
       count(if(velocity > 0, 1, null)),
       sum(if(dif_vel>500 or dif_vel<-500,1,0)) danger_count,
       dt
from (select vin,
             mileage,
             velocity,
             velocity -  lag(velocity,1,velocity)over (partition by  dt,vin order by \`timestamp\`) dif_vel,
             dt
      from ${APP}.dwd_car_running_electricity_inc
      where dt<=${do_date}
) t1
group by vin, dt;
"

dws_car_alarm_1d="insert into ${APP}.dws_car_alarm_1d partition (dt)
select vin,
       count(*),
       sum(if(alarm_level = 1, 1, 0)),
       sum(if(alarm_level = 2, 1, 0)),
       sum(if(alarm_level = 3, 1, 0)),
       dt
from ${APP}.dwd_car_alarm_inc
where dt<=${do_date}
group by vin,dt;
"

dws_car_motor_1d="insert overwrite table ${APP}.dws_car_motor_1d partition (dt)
select
    vin,
    max(temperature) max_motor_temperature,
    avg(temperature) avg_motor_temperature,
    max(controller_temperature) max_motor_controller_temperature,
    avg(controller_temperature) avg_motor_controller_temperature,
    dt
from ${APP}.dwd_car_motor_inc
where dt<=${do_date}
group by dt,vin;
"

dws_car_battery_1d="
with t1 as (
    select
        vin,
        max(max_temperature) max_battery_temperature ,
        sum(if(max_temperature > 600,1,0))  battery_temperature_abnormal_count,
        avg(voltage) avg_voltage_discharge ,
        dt
    from ${APP}.dwd_car_running_electricity_inc
    where dt<=${do_date}
    group by vin,dt
),t2 as (
    select
        vin,
        max(max_temperature) max_battery_temperature ,
        sum(if(max_temperature > 600,1,0))  battery_temperature_abnormal_count,
        avg(voltage) avg_voltage_charge ,
        dt
    from ${APP}.dwd_car_parking_charging_inc
    where dt<=${do_date}
    group by vin,dt
),t3 as (
    select
        vin,
        sum(start_soc-end_soc)  soc_consumed,
        dt
    from ${APP}.dws_electricity_single_trip_detail
    where dt<=${do_date}
    group by vin,dt
)
insert overwrite table ${APP}.dws_car_battery_1d partition(dt)
select
    nvl(t1.vin,t2.vin),
    if( nvl(t1.max_battery_temperature,0)>nvl(t2.max_battery_temperature,0),nvl(t1.max_battery_temperature,0),nvl(t2.max_battery_temperature,0) ) max_battery_temperature,
    nvl(t1.battery_temperature_abnormal_count,0) + nvl(t2.battery_temperature_abnormal_count,0) battery_temperature_abnormal_count,
    nvl(avg_voltage_charge,0) avg_voltage_charge,
    nvl(avg_voltage_discharge,0) avg_voltage_discharge,
    nvl(soc_consumed,0) soc_consumed,
    nvl(t1.dt,t2.dt)
from t1
full outer join t2
on t1.vin=t2.vin and t1.dt=t2.vin
left join t3
on t1.vin=t3.vin and t1.dt=t3.dt;
"

case $1 in
'dws_car_mileage_1d')
  hive -e "$dws_car_mileage_1d"
  ;;
'dws_car_alarm_1d')
  hive -e "$dws_car_alarm_1d"
  ;;
'dws_car_motor_1d')
  hive -e "$dws_car_motor_1d"
  ;;
'dws_car_battery_1d')
  hive -e "$dws_car_battery_1d"
  ;;
"all")
  hive -e "$dws_car_mileage_1d$dws_car_alarm_1d$dws_car_motor_1d$dws_car_battery_1d"
  ;;
esac
