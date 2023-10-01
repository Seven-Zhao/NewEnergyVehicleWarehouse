-- 9.2 最近 1 日汇总表
-- 9.2.1 汽车行程单日累计表
-- 1）建表语句
drop table if exists dws_car_mileage_1d;
create table dws_car_mileage_1d
(
    vin           string comment '汽车唯一ID',
    total_mileage int comment '一日累计里程',
    total_speed   int comment '平均时速分子',
    running_count int comment '平均时速分母',
    danger_count  int comment '单日急加减速次数'
)
    comment '汽车行程单日累计表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dws/dws_car_mileage_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 2）数据装载
-- （1）首日装载
insert overwrite table dws_car_mileage_1d partition (dt)
select vin,
       max(mileage) - min(mileage)                      total_mileage,
       sum(velocity)                                    total_speed,
       sum(`if`(velocity > 0, 1, 0))                    running_count,
       sum(`if`(dif_vel > 500 or dif_vel < -500, 1, 0)) danger_count,
       dt
from (select vin,
             mileage,
             velocity,
             `timestamp`,
             velocity - lag(velocity, 1, velocity) over (partition by dt,vin order by `timestamp`) dif_vel,
             dt
      from dwd_car_running_electricity_inc
      where dt <= '2023-05-02') t1
group by dt, vin;
-- （2）每日装载
insert overwrite table dws_car_mileage_1d partition (dt = '2023-05-03')
select vin,
       max(mileage) - min(mileage)                      total_mileage,
       sum(velocity)                                    total_speed,
       sum(`if`(velocity > 0, 1, 0))                    running_count,
       sum(`if`(dif_vel > 500 or dif_vel < -500, 1, 0)) danger_count
from (select vin,
             mileage,
             velocity,
             `timestamp`,
             velocity - lag(velocity, 1, velocity) over (partition by dt,vin order by `timestamp`) dif_vel,
             dt
      from dwd_car_running_electricity_inc
      where dt = '2023-05-03') t1
group by vin;