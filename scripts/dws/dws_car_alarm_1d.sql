-- 9.2.2 汽车告警单日累计表
-- 1）建表语句
drop table if exists dws_car_alarm_1d;
create table dws_car_alarm_1d
(
    vin            string comment '汽车唯一ID',
    alarm_count    int comment '告警次数',
    l1_alarm_count int comment '一级告警次数',
    l2_alarm_count int comment '二级告警次数',
    l3_alarm_count int comment '三级告警次数'
)
    comment '汽车告警单日累计表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dws/dws_car_alarm_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 2）数据装载
-- （1）首日装载
insert into dws_car_alarm_1d partition (dt)
select vin,
       count(*),
       sum(if(alarm_level = 1, 1, 0)),
       sum(if(alarm_level = 2, 1, 0)),
       sum(if(alarm_level = 3, 1, 0)),
       dt
from dwd_car_alarm_inc
where dt <= '2023-05-02'
group by vin, dt;
-- （2）每日装载
insert into dws_car_alarm_1d partition (dt = '2023-05-03')
select vin,
       count(*),
       sum(if(alarm_level = 1, 1, 0)),
       sum(if(alarm_level = 2, 1, 0)),
       sum(if(alarm_level = 3, 1, 0))
from dwd_car_alarm_inc
where dt = '2023-05-03'
group by vin;