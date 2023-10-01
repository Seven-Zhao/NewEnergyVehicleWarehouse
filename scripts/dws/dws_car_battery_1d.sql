-- 9.2.4 汽车电池组信息汇总
-- 1）建表语句
drop table if exists dws_car_battery_1d;
create table dws_car_battery_1d
(
    vin                                string comment '汽车唯一ID',
    max_battery_temperature            int comment '最高电池温度',
    battery_temperature_abnormal_count int comment '电池温度异常值次数',
    avg_voltage_charge                 decimal(16, 2) comment '充电平均电压',
    avg_voltage_discharge              decimal(16, 2) comment '放电平均电压',
    soc_consumed                       int comment '每日累计soc消耗'
)
    comment '汽车电池单日累计表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dws/dws_car_battery_1d'
    tblproperties ('orc.compress' = 'snappy');
-- 2）数据装载
-- （1）首日装载
with t1 as (select vin,
                   max(max_temperature)                 max_battery_temperature,
                   sum(if(max_temperature > 600, 1, 0)) battery_temperature_abnormal_count,
                   avg(voltage)                         avg_voltage_discharge,
                   dt
            from dwd_car_running_electricity_inc
            where dt <= '2023-05-02'
            group by vin, dt),
     t2 as (select vin,
                   max(max_temperature)                 max_battery_temperature,
                   sum(if(max_temperature > 600, 1, 0)) battery_temperature_abnormal_count,
                   avg(voltage)                         avg_voltage_charge,
                   dt
            from dwd_car_parking_charging_inc
            where dt <= '2023-05-02'
            group by vin, dt),
     t3 as (select vin,
                   sum(start_soc - end_soc) soc_consumed,
                   dt
            from dws_electricity_single_trip_detail
            where dt <= '2023-05-02'
            group by vin, dt)
insert
overwrite
table
dws_car_battery_1d
partition
(
dt
)
select nvl(t1.vin, t2.vin),
       if(nvl(t1.max_battery_temperature, 0) > nvl(t2.max_battery_temperature, 0), nvl(t1.max_battery_temperature, 0),
          nvl(t2.max_battery_temperature, 0))        as max_battery_temperature,
       nvl(t1.battery_temperature_abnormal_count, 0) +
       nvl(t2.battery_temperature_abnormal_count, 0) as battery_temperature_abnormal_count,
       nvl(avg_voltage_charge, 0)                    as avg_voltage_charge,
       nvl(avg_voltage_discharge, 0)                 as avg_voltage_discharge,
       nvl(soc_consumed, 0)                          as soc_consumed,
       nvl(t1.dt, t2.dt)
from t1
         full outer join t2
                         on t1.vin = t2.vin and t1.dt = t2.vin
         left join t3
                   on t1.vin = t3.vin and t1.dt = t3.dt;
-- （2）每日装载
insert overwrite table dws_car_battery_1d partition (dt = '2023-05-03')
with t1 as (select vin,
                   max(max_temperature)                 max_battery_temperature,
                   sum(if(max_temperature > 600, 1, 0)) battery_temperature_abnormal_count,
                   avg(voltage)                         avg_voltage_discharge,
                   dt
            from dwd_car_running_electricity_inc
            where dt = '2023-05-03'
            group by vin, dt),
     t2 as (select vin,
                   max(max_temperature)                 max_battery_temperature,
                   sum(if(max_temperature > 600, 1, 0)) battery_temperature_abnormal_count,
                   avg(voltage)                         avg_voltage_charge,
                   dt
            from dwd_car_parking_charging_inc
            where dt = '2023-05-03'
            group by vin, dt),
     t3 as (select vin,
                   sum(start_soc - end_soc) soc_consumed,
                   dt
            from dws_electricity_single_trip_detail
            where dt = '2023-05-03'
            group by vin, dt)
select nvl(t1.vin, t2.vin),
       if(nvl(t1.max_battery_temperature, 0) > nvl(t2.max_battery_temperature, 0), nvl(t1.max_battery_temperature, 0),
          nvl(t2.max_battery_temperature, 0))        as max_battery_temperature,
       nvl(t1.battery_temperature_abnormal_count, 0) +
       nvl(t2.battery_temperature_abnormal_count, 0) as battery_temperature_abnormal_count,
       nvl(avg_voltage_charge, 0)                    as avg_voltage_charge,
       nvl(avg_voltage_discharge, 0)                 as avg_voltage_discharge,
       nvl(soc_consumed, 0)                          as soc_consumed
from t1
         full outer join t2
                         on t1.vin = t2.vin and t1.dt = t2.vin
         left join t3
                   on t1.vin = t3.vin and t1.dt = t3.dt;