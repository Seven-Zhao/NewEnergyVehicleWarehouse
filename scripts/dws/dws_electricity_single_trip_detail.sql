-- 9.1 单次粒度汇总表
-- 9.1.1 单次行程汇总表
-- 1）建表语句
drop table if exists dws_electricity_single_trip_detail;
create external table dws_electricity_single_trip_detail
(
    `id`                          string comment '行程id',
    `vin`                         string comment '汽车唯一编码',
    `start_timestamp`             bigint comment '开始时间',
    `end_timestamp`               bigint comment '结束时间',
    `start_mileage`               int comment '开始里程',
    `end_mileage`                 int comment '结束里程',
    `start_soc`                   int comment '行程开始soc, 单位：0.1%',
    `end_soc`                     int comment '行程结束soc, 单位：0.1%',
    `avg_speed`                   decimal(16, 2) comment '平均时速',
    `car_avg_voltage`             decimal(16, 2) comment '汽车平均电压',
    `car_avg_electric_current`    decimal(16, 2) comment '汽车平均电流',
    `battery_avg_max_temperature` decimal(16, 2) comment '电池组最高温度的平均温度',
    `battery_mid_max_temperature` decimal(16, 2) comment '电池组最高温度的中位数',
    `battery_avg_min_temperature` decimal(16, 2) comment '电池组最低温度的平均温度',
    `battery_mid_min_temperature` decimal(16, 2) comment '电池组最低温度的中位数',
    `battery_avg_max_voltage`     decimal(16, 2) comment '电池组最高电压的平均电压',
    `battery_mid_max_voltage`     decimal(16, 2) comment '电池组最高电压的中位数',
    `battery_avg_min_voltage`     decimal(16, 2) comment '电池组最低电压的平均电压',
    `battery_mid_min_voltage`     decimal(16, 2) comment '电池组最低电压的中位数'
) comment '单次行程汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dws/dws_electricity_single_trip_detail'
    tblproperties ('orc.compress' = 'snappy');
-- 2）首日装载
insert overwrite table dws_electricity_single_trip_detail partition (dt)
select concat(vin, '-', min(`timestamp`))                       id,
       vin,
       min(`timestamp`)                                         start_timestamp,
       max(`timestamp`)                                         `end_timestamp`,
       min(mileage)                                             `start_mileage`,
       max(mileage)                                             `end_mileage`,
       max(soc)                                                 `start_soc`,
       min(soc)                                                 `end_soc`,
       avg(velocity)                                            `avg_speed`,
       avg(voltage)                                             `car_avg_voltage`,
       avg(electric_current)                                    `car_avg_electric_current`,
       avg(max_temperature)                                     `battery_avg_max_temperature`,
       collect_list(max_temperature)[cast(count(*) / 2 as int)] `battery_mid_max_temperature`,
       avg(min_temperature)                                     `battery_avg_min_temperature`,
       collect_list(min_temperature)[cast(count(*) / 2 as int)] ` battery_mid_min_temperature`,
       avg(max_voltage)                                         `battery_avg_max_voltage`,
       collect_list(max_voltage)[cast(count(*) / 2 as int)]     ` battery_mid_max_voltage`,
       avg(min_voltage)                                         `battery_avg_min_voltage`,
       collect_list(min_voltage)[cast(count(*) / 2 as int)]     `battery_mid_min_voltage`,
       dt
from (select vin,
             velocity,
             mileage,
             voltage,
             electric_current,
             soc,
             max_temperature,
             max_voltage,
             min_temperature,
             min_voltage,
             `timestamp`,
             dt,
             sum(mark) over (partition by vin order by `timestamp`) singer_trip
      from (select vin,
                   velocity,
                   mileage,
                   voltage,
                   electric_current,
                   soc,
                   max_temperature,
                   max_voltage,
                   min_temperature,
                   min_voltage,
                   `timestamp`,
                   dt,
                   if((lag(`timestamp`, 1, 0) over (partition by vin order by `timestamp` ) - `timestamp`) < -60000, 1,
                      0) mark
            from dwd_car_running_electricity_inc
            where dt <= '2023-05-02') t1) t2
group by dt, vin, singer_trip;

-- 3）每日装载
insert overwrite table dws_electricity_single_trip_detail partition (dt = '2023-05-03')
select concat(vin, '-', min(`timestamp`))                       id,
       vin,
       min(`timestamp`)                                         start_timestamp,
       max(`timestamp`)                                         `end_timestamp`,
       min(mileage)                                             `start_mileage`,
       max(mileage)                                             `end_mileage`,
       max(soc)                                                 `start_soc`,
       min(soc)                                                 `end_soc`,
       avg(velocity)                                            `avg_speed`,
       avg(voltage)                                             `car_avg_voltage`,
       avg(electric_current)                                    `car_avg_electric_current`,
       avg(max_temperature)                                     `battery_avg_max_temperature`,
       collect_list(max_temperature)[cast(count(*) / 2 as int)] `battery_mid_max_temperature`,
       avg(min_temperature)                                     `battery_avg_min_temperature`,
       collect_list(min_temperature)[cast(count(*) / 2 as int)] ` battery_mid_min_temperature`,
       avg(max_voltage)                                         `battery_avg_max_voltage`,
       collect_list(max_voltage)[cast(count(*) / 2 as int)]     ` battery_mid_max_voltage`,
       avg(min_voltage)                                         `battery_avg_min_voltage`,
       collect_list(min_voltage)[cast(count(*) / 2 as int)]     `battery_mid_min_voltage`
from (select vin,
             velocity,
             mileage,
             voltage,
             electric_current,
             soc,
             max_temperature,
             max_voltage,
             min_temperature,
             min_voltage,
             `timestamp`,
             dt,
             sum(mark) over (partition by vin order by `timestamp`) singer_trip
      from (select vin,
                   velocity,
                   mileage,
                   voltage,
                   electric_current,
                   soc,
                   max_temperature,
                   max_voltage,
                   min_temperature,
                   min_voltage,
                   `timestamp`,
                   dt,
                   if((lag(`timestamp`, 1, 0) over (partition by vin order by `timestamp` ) - `timestamp`) < -60000, 1,
                      0) mark
            from dwd_car_running_electricity_inc
            where dt = '2023-05-03') t1) t2
group by vin, singer_trip;