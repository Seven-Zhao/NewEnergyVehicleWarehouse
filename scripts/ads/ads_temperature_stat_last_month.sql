-- 10.3 温控主题
-- 10.3.1 温控情况单月统计
-- 1）建表语句
drop table if exists ads_temperature_stat_last_month;
create external table ads_temperature_stat_last_month
(
    vin                                string comment '汽车唯一ID',
    mon          string comment '统计月份',
    max_motor_temperature              int comment '电机最高温度',
    avg_motor_temperature              decimal(16, 2) comment '电机平均温度',
    max_motor_controller_temperature   int comment '电机控制器最高温度',
    avg_motor_controller_temperature   decimal(16, 2) comment '电机控制器平均温度',
    max_battery_temperature            int comment '最高电池温度',
    battery_temperature_abnormal_count int comment '电池温度异常值次数'
) comment '温控相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_temperature_stat_last_month';

-- 2）数据装载
insert overwrite table ads_temperature_stat_last_month
select *
from ads_temperature_stat_last_month
union
select motor.vin,
       '2023-05'                                                   mon,
       max_motor_temperature,
       cast(avg_motor_temperature as decimal(16, 2))            as avg_motor_temperature,
       max_motor_controller_temperature,
       cast(avg_motor_controller_temperature as decimal(16, 2)) as avg_motor_controller_temperature,
       battery_temperature_abnormal_count
from (select vin,
             max(max_motor_temperature)            max_motor_temperature,
             avg(avg_motor_temperature)            avg_motor_temperature,
             max(max_motor_controller_temperature) max_motor_controller_temperature,
             avg(avg_motor_controller_temperature) avg_motor_controller_temperature
      from dws_car_motor_1d
      where substr(dt, 0, 7) = '2023-05'
      group by vin) motor
         join
     (select vin,
             max(max_battery_temperature)            max_battery_temperature,
             sum(battery_temperature_abnormal_count) battery_temperature_abnormal_count
      from dws_car_battery_1d
      where substr(dt, 0, 7) = '2023-05'
      group by vin) battery
     on motor.vin = battery.vin;