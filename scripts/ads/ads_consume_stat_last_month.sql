-- 10.4 能耗主题
-- 10.4.1 能耗相关分析
-- 1）建表语句
drop table if exists ads_consume_stat_last_month;
create external table ads_consume_stat_last_month
(
    vin                 string comment '汽车唯一ID',
    mon                 string comment '统计月份',
    soc_per_charge      decimal(16, 2) comment '次均充电电量',
    duration_per_charge decimal(16, 2) comment '次均充电时长',
    charge_count        int comment '充电次数',
    fast_charge_count   int comment '快充次数',
    slow_charge_count   int comment '慢充次数',
    fully_charge_count  int comment '深度充电次数',
    soc_per_100km       decimal(16, 2) comment 'soc百公里平均消耗',
    soc_per_run         decimal(16, 2) comment '每次里程soc平均消耗',
    soc_last_100km      decimal(16, 2) comment '最近百公里soc消耗'
) comment '能耗主题统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_consume_stat_last_month';
-- 2）数据装载
with single_trip as (select vin,
                            start_soc,
                            end_soc,
                            start_mileage,
                            end_mileage,
                            start_timestamp,
                            end_timestamp,
                            dt
                     from dws_electricity_single_trip_detail
                     where substr(dt, 1, 7) = '2023-05'),
     a0 as (select vin,
                   avg(soc)                            soc_per_charge,
                   avg(duration)                       duration_per_charge,
                   count(*)                            charge_count,
                   sum(`if`(avg_current >= 180, 1, 0)) fast_charge_count,
                   sum(`if`(avg_current < 180, 1, 0))  slow_charge_count,
                   sum(fully_charge)                   fully_charge_count
            from (select vin,
                         max(soc) - min(soc)                           soc,
                         (max(`timestamp`) - min(`timestamp`)) / 1000  duration,
                         avg(electric_current)                         avg_current,
                         `if`(min(soc) <= 20 and max(soc) >= 80, 1, 0) fully_charge
                  from (select vin,
                               soc,
                               electric_current,
                               `timestamp`,
                               sum(`if`(dif_ts > 60000, 1, 0)) over (partition by vin order by `timestamp`) mark
                        from (select vin,
                                     soc,
                                     electric_current,
                                     `timestamp`,
                                     `timestamp` -
                                     lag(`timestamp`, 1, 0) over (partition by vin order by `timestamp`) dif_ts
                              from dwd_car_parking_charging_inc
                              where substr(dt, 0, 7) = '2023-05') a1) a2
                  group by vin, mark) a3
            group by vin)
insert
overwrite
table
ads_consume_stat_last_month
select *
from ads_consume_stat_last_month
union
select a0.vin,
       '2023-05' mon,
       cast(nvl(soc_per_charge, 0) as decimal(16, 2)),
       cast(nvl(duration_per_charge, 0) as decimal(16, 2)),
       charge_count,
       fast_charge_count,
       slow_charge_count,
       fully_charge_count,
       cast(nvl(soc_per_100km, 0) as decimal(16, 2)),
       cast(nvl(soc_per_run, 0) as decimal(16, 2)),
       cast(nvl(soc_last_100km, 0) as decimal(16, 2))
from (select vin,
             sum(start_soc - end_soc) / sum(end_mileage - start_mileage) * 1000          soc_per_100km,
             avg(start_soc - end_soc)                                                    soc_per_run,
             sum(`if`(lag_sum_mi < 1000 and last_sum_mi >= 1000, last_sum_soc, 0)) /
             sum(`if`(lag_sum_mi < 1000 and last_sum_mi >= 1000, last_sum_mi, 0)) * 1000 soc_last_100km
      from (select vin,
                   start_soc,
                   end_soc,
                   start_mileage,
                   end_mileage,
                   end_timestamp,
                   last_sum_mi,
                   last_sum_soc,
                   lag(last_sum_mi, 1, 0) over (partition by vin order by end_timestamp desc) lag_sum_mi
            from (select vin,
                         start_soc,
                         end_soc,
                         start_mileage,
                         end_mileage,
                         end_timestamp,
                         sum(end_mileage - start_mileage)
                             over (partition by vin order by end_timestamp desc )                      last_sum_mi,
                         sum(start_soc - end_soc) over (partition by vin order by end_timestamp desc ) last_sum_soc
                  from (select t1.vin,
                               t1.start_soc,
                               nvl(t2.end_soc, t1.end_soc)                                                 end_soc,
                               t1.start_mileage,
                               nvl(t2.end_mileage, t1.end_mileage)                                         end_mileage,
                               t1.start_timestamp,
                               nvl(t2.end_timestamp, t1.end_timestamp)                                     end_timestamp,
                               lag(t2.vin, 1, null) over (partition by t1.vin order by t1.start_timestamp) del_mark
                        from single_trip t1
                                 left join single_trip t2
                                           on t1.vin = t2.vin and datediff(t2.dt, t1.dt) = 1 and
                                              t2.start_timestamp - t1.end_timestamp = 30000) t3
                  where del_mark is null) t4) t5
      group by vin) t6
         join a0
              on t6.vin = a0.vin;