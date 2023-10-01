-- 10.1.2 车型里程相关统计
-- 1）建表语句
drop table if exists ads_car_type_mileage_stat_last_month;
create external table ads_car_type_mileage_stat_last_month
(
    car_type     string comment '车型',
    mon          string comment '统计月份',
    avg_mileage  int comment '日均里程',
    avg_speed    decimal(16, 2) comment '平均时速分子',
    danger_count decimal(16, 2) comment '平均百公里急加减速次数'
) comment '车型里程相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_car_type_mileage_stat_last_month';

-- 2）数据装载
insert overwrite table ads_car_type_mileage_stat_last_month
select *
from ads_car_type_mileage_stat_last_month
union
select type,
       '2023-05',
       cast(avg(total_mileage) as int)                                       as avg_mileage,
       cast(sum(total_speed) / sum(running_count) as decimal(16, 2))         as avg_speed,
       cast(sum(danger_count) / sum(total_mileage) * 1000 as decimal(16, 2)) as danger_count
from dws_car_mileage_1d s
         join (select id,
                      type
               from dim_car_info_full
               where dt = '2023-05-02') car_info
              on s.vin = car_info.id
where substr(s.dt, 1, 7) = '2023-05'
group by type;