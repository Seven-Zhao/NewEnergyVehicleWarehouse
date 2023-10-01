-- 3）最近7日汇总建表语句
drop table if exists ads_mileage_stat_last_7d;
create external table ads_mileage_stat_last_7d
(
    vin          string comment '汽车唯一ID',
    dt           string comment '统计日期',
    avg_mileage  int comment '日均里程',
    avg_speed    decimal(16, 2) comment '平均时速分子',
    danger_count decimal(16, 2) comment '平均百公里急加减速次数'
) comment '里程相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_mileage_stat_last_7d';

-- 4）最近7日汇总数据装载
insert overwrite table ads_mileage_stat_last_7d
select *
from ads_mileage_stat_last_7d
union
select vin,
       '2023-05-02',
       cast(avg(total_mileage) as int)                               avg_mileage,
       cast(sum(total_speed) / sum(running_count) as decimal(16, 2)) avg_speed,
       cast(nvl(sum(danger_count) / sum(total_mileage) * 1000, 0) as decimal(16, 2))
from dws_car_mileage_1d
where dt <= '2023-05-02'
  and dt > date_sub('2023-05-02', 7)
group by vin;