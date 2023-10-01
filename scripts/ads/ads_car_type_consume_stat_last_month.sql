-- 10.4.2 车型能耗相关统计
-- 1）建表语句
drop table if exists ads_car_type_consume_stat_last_month;
create external table ads_car_type_consume_stat_last_month
(
    car_type                string comment '车型',
    mon                     string comment '统计月份',
    avg_soc_per_charge      decimal(16, 2) comment '平均每次充电电量',
    avg_duration_per_charge decimal(16, 2) comment '平均每次充电时长',
    avg_charge_count        int comment '平均充电次数',
    avg_fast_charge_count   int comment '平均快充次数',
    avg_slow_charge_count   int comment '平均慢充次数',
    avg_fully_charge_count  int comment '平均深度充电次数',
    soc_per_100km           decimal(16, 2) comment 'soc百公里平均消耗',
    soc_per_run             decimal(16, 2) comment '每次里程soc平均消耗',
    soc_last_100km          decimal(16, 2) comment '最近百公里soc平均消耗'
) comment '车型能耗主题统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_car_type_consume_stat_last_month';

-- 2）数据装载
insert overwrite table ads_car_type_consume_stat_last_month
select *
from ads_car_type_consume_stat_last_month
union
select type,
       '2023-05'                                        as mon,
       cast(avg(soc_per_charge) as decimal(16, 2))      avg_soc_per_charge,
       cast(avg(duration_per_charge) as decimal(16, 2)) avg_duration_per_charge,
       cast(avg(charge_count) as int)                   avg_charge_count,
       cast(avg(fast_charge_count) as int)              avg_fast_charge_count,
       cast(avg(slow_charge_count) as int)              avg_slow_charge_count,
       cast(avg(fully_charge_count) as int)             avg_fully_charge_count,
       cast(avg(soc_per_100km) as decimal(16, 2))       as soc_per_100km,
       cast(avg(soc_per_run) as decimal(16, 2))         as soc_per_run,
       cast(avg(soc_last_100km) as decimal(16, 2))      as soc_last_100km
from (select vin,
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
      from ads_consume_stat_last_month
      where mon = '2023-05') consume
         join (select id,
                      type
               from dim_car_info_full
               where dt = '2023-05-03') car_info
              on vin = id
group by type;