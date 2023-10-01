#!/usr/bin/env bash

APP='car_data'

if [ -n "$2" ]; then
  do_date=$2
else
  do_date=$(date -d '-1 day' +%F)
fi
do_month=$(date -d "${do_date}" +"%Y-%m")

database_init="use car_data;"
ads_mileage_stat_last_month="
insert overwrite table ${APP}.ads_mileage_stat_last_month
select *
from ${APP}.ads_mileage_stat_last_month
union
select vin,
       '${do_month}',
       cast(avg(total_mileage) as int) avg_mileage,
       cast(sum(total_speed)/sum(running_count) as decimal(16,2)) avg_speed,
       cast(nvl(sum(danger_count) / sum(total_mileage) * 1000 ,0) as decimal(16,2))
from ${APP}.dws_car_mileage_1d
where substr(dt, 0, 7) = '${do_month}'
group by vin;
"

ads_car_type_mileage_stat_last_month="
insert overwrite table ${APP}.ads_car_type_mileage_stat_last_month
select *
from ${APP}.ads_car_type_mileage_stat_last_month
union
select type,
        '${do_month}',
        cast(avg(total_mileage) as int) avg_mileage,
        cast(sum(total_speed)/sum(running_count) as decimal(16,2)) avg_speed,
cast( sum(danger_count)/sum(total_mileage) * 1000 as decimal(16,2)) danger_count
from ${APP}.dws_car_mileage_1d s
          join (
             select
                id,
                type
             from ${APP}.dim_car_info_full
             where dt='${do_date}'
        )car_info
        on s.vin=car_info.id
where substr(s.dt, 1, 7) = '${do_month}'
group by type;
"
ads_alarm_stat_last_month="
insert overwrite table ${APP}.ads_alarm_stat_last_month
select *
from ${APP}.ads_alarm_stat_last_month
union
select vin,
       '${do_month}',
       sum(alarm_count),
       sum(l1_alarm_count),
       sum(l2_alarm_count),
       sum(l3_alarm_count)
from ${APP}.dws_car_alarm_1d
where substr(dt, 0, 7) = '${do_month}'
group by vin;
"

ads_temperature_stat_last_month="
insert overwrite table ${APP}.ads_temperature_stat_last_month
select *
from ${APP}.ads_temperature_stat_last_month
union
select
    motor.vin,
    '${do_month}' mon,
    max_motor_temperature,
    cast(avg_motor_temperature as decimal(16,2)) avg_motor_temperature,
    max_motor_controller_temperature,
    cast(avg_motor_controller_temperature as decimal(16,2)) avg_motor_controller_temperature,
    max_battery_temperature,
    battery_temperature_abnormal_count
from (
     select
        vin,
        max(max_motor_temperature) max_motor_temperature,
        avg(avg_motor_temperature) avg_motor_temperature,
        max(max_motor_controller_temperature) max_motor_controller_temperature,
        avg(avg_motor_controller_temperature) avg_motor_controller_temperature
    from ${APP}.dws_car_motor_1d
    where substr(dt, 0, 7) = '${do_month}'
    group by vin
)motor
join
    (
    select
        vin,
        max(max_battery_temperature) max_battery_temperature,
        sum(battery_temperature_abnormal_count) battery_temperature_abnormal_count
    from ${APP}.dws_car_battery_1d
    where substr(dt, 0, 7) = '${do_month}'
    group by vin
)battery
on motor.vin=battery.vin;
"

ads_consume_stat_last_month="
with single_trip as (
    select
        vin,
        start_soc,
        end_soc,
        start_mileage,
        end_mileage,
        start_timestamp,
        end_timestamp,
        dt
    from ${APP}.dws_electricity_single_trip_detail
    where substr(dt,1,7)='${do_month}'
),a0  as (
    select
        vin,
        avg(soc) soc_per_charge,
        avg(duration) duration_per_charge,
        count(*) charge_count,
        sum(if(avg_current>=180,1,0)) fast_charge_count,
        sum(if(avg_current<180,1,0))  slow_charge_count,
        sum(fully_charge) fully_charge_count
    from (
         select
            vin,
            max(soc) - min(soc) soc,
            (max(\`timestamp\`) - min(\`timestamp\`))/1000 duration,
            avg(electric_current) avg_current,
            if(min(soc)<=20 and max(soc)>=80,1,0) fully_charge
        from (
             select
                vin,
                soc,
                electric_current,
                \`timestamp\`,
                sum(if(dif_ts > 60000,1,0)) over (partition by vin order by \`timestamp\`) mark
            from (
                 select
                    vin,
                    soc,
                    electric_current,
                    \`timestamp\`,
                   \`timestamp\`- lag(\`timestamp\`,1,0) over (partition by vin order by \`timestamp\`) dif_ts
                from ${APP}.dwd_car_parking_charging_inc
                where substr(dt,0,7)='${do_month}'
            )a1
        )a2
        group by vin,mark
    )a3
    group by vin
)
insert overwrite table ${APP}.ads_consume_stat_last_month
select *
from ${APP}.ads_consume_stat_last_month
union
select
    a0.vin,
    '${do_month}' mon,
    cast(nvl(soc_per_charge,0) as decimal(16,2)),
    cast(nvl(duration_per_charge,0) as decimal(16,2)),
    charge_count,
    fast_charge_count,
    slow_charge_count,
    fully_charge_count,
    cast(nvl(soc_per_100km,0) as decimal(16,2)),
    cast(nvl(soc_per_run,0) as decimal(16,2)),
    cast(nvl(soc_last_100km,0) as decimal(16,2))
from (
     select
        vin,
        sum(start_soc - end_soc) / sum(end_mileage-start_mileage) * 1000  soc_per_100km,
        avg(start_soc - end_soc) soc_per_run,
        sum(if( lag_sum_mi<1000 and last_sum_mi >= 1000,last_sum_soc,0)) /
        sum(if( lag_sum_mi<1000 and last_sum_mi >= 1000,last_sum_mi,0))  * 1000 soc_last_100km
    from (
         select
            vin,
            start_soc,
            end_soc,
            start_mileage,
            end_mileage,
            end_timestamp,
            last_sum_mi,
            last_sum_soc,
            lag(last_sum_mi,1,0)over (partition by vin order by end_timestamp desc) lag_sum_mi
        from (
             select
                vin,
                start_soc,
                end_soc,
                start_mileage,
                end_mileage,
                end_timestamp,
                sum(end_mileage - start_mileage) over (partition by vin order by end_timestamp desc ) last_sum_mi,
                sum(start_soc - end_soc) over (partition by vin order by end_timestamp desc ) last_sum_soc
            from (
                 select
                    t1.vin,
                    t1.start_soc,
                    nvl(t2.end_soc,t1.end_soc) end_soc,
                    t1.start_mileage,
                    nvl(t2.end_mileage,t1.end_mileage) end_mileage,
                    t1.start_timestamp,
                    nvl(t2.end_timestamp,t1.end_timestamp) end_timestamp,
                    lag(t2.vin,1,null)over (partition by t1.vin order by t1.start_timestamp) del_mark
                from single_trip t1
                left join single_trip t2
                on t1.vin=t2.vin and  datediff(t2.dt,t1.dt)=1 and t2.start_timestamp-t1.end_timestamp=30000
            )t3
            where del_mark is null
        )t4
    )t5
    group by vin
)t6 join a0
on t6.vin=a0.vin;
"

ads_car_type_consume_stat_last_month="
insert overwrite table ${APP}.ads_car_type_consume_stat_last_month
select *
from ${APP}.ads_car_type_consume_stat_last_month
union
select
    type,
    '${do_month}' mon,
    cast(avg(soc_per_charge) as decimal(16,2)) avg_soc_per_charge,
    cast(avg(duration_per_charge) as decimal(16,2)) avg_duration_per_charge,
    cast(avg(charge_count) as int) avg_charge_count,
    cast(avg(fast_charge_count) as int) avg_fast_charge_count,
    cast(avg(slow_charge_count) as int ) avg_slow_charge_count,
    cast( avg(fully_charge_count) as int ) avg_fully_charge_count,
    cast(avg(soc_per_100km) as decimal(16,2)) soc_per_100km,
    cast(avg(soc_per_run) as decimal(16,2)) soc_per_run,
    cast(avg(soc_last_100km) as decimal(16,2))soc_last_100km
from (
     select
        vin,
        mon,
        soc_per_charge,
        duration_per_charge,
        charge_count,
        fast_charge_count,
        slow_charge_count,
        fully_charge_count,
        soc_per_100km,
        soc_per_run,
        soc_last_100km
    from ${APP}.ads_consume_stat_last_month
    where mon='${do_month}'
)consume
join (
    select
        id,
        type
    from ${APP}.dim_car_info_full
    where dt='${do_date}'
)car_info
on vin=id
group by type;
"

case $1 in
'ads_mileage_stat_last_month')
  hive -e "$ads_mileage_stat_last_month"
  ;;
'ads_car_type_mileage_stat_last_month')
  hive -e "$ads_car_type_mileage_stat_last_month"
  ;;
'ads_alarm_stat_last_month')
  hive -e "$ads_alarm_stat_last_month"
  ;;
'ads_temperature_stat_last_month')
  hive -e "$ads_temperature_stat_last_month"
  ;;
'ads_consume_stat_last_month')
  hive -e "$ads_consume_stat_last_month"
  ;;
'ads_car_type_consume_stat_last_month')
  hive -e "$ads_car_type_consume_stat_last_month"
  ;;
"all")
  hive -e "${ads_mileage_stat_last_month}${ads_car_type_mileage_stat_last_month}${ads_alarm_stat_last_month}${ads_temperature_stat_last_month}${ads_consume_stat_last_month}${ads_car_type_consume_stat_last_month}"
  ;;
esac
