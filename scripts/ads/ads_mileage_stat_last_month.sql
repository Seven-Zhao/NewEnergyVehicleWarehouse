-- 10.1行程主题
-- 10.1.1行程相关统计
-- 1）当月汇总建表语句
drop table if exists ads_mileage_stat_last_month;
create external table ads_mileage_stat_last_month
(
    vin          string comment '汽车唯一ID',
    mon          string comment '统计月份',
    avg_mileage  int comment '日均里程',
    avg_speed    decimal(16, 2) comment '平均时速',
    danger_count decimal(16, 2) comment '平均百公里急加减速次数'
) comment '里程相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_mileage_stat_last_month';

-- 2）当月汇总数据装载
insert overwrite table ads_mileage_stat_last_month
select *
from ads_mileage_stat_last_month
union
select vin,
       '2023-05',
       cast(avg(total_mileage) as int)                               avg_mileage,
       cast(sum(total_speed) / sum(running_count) as decimal(16, 2)) avg_speed,
       cast(nvl(sum(danger_count) / sum(total_mileage) * 1000, 0) as decimal(16, 2))
from dws_car_mileage_1d
where substr(dt, 0, 7) = '2023-05'
group by vin;