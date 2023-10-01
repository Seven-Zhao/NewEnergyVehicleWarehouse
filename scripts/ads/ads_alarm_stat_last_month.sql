-- 10.2 告警主题
-- 10.2.1 告警情况单月统计
-- 1）建表语句
drop table if exists ads_alarm_stat_last_month;
create external table ads_alarm_stat_last_month
(
    vin            string comment '汽车唯一ID',
    mon            string comment '统计月份',
    alarm_count    int comment '告警次数',
    l1_alarm_count int comment '一级告警次数',
    l2_alarm_count int comment '二级告警次数',
    l3_alarm_count int comment '三级告警次数'
) comment '告警相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/car_data/ads/ads_alarm_stat_last_month';
-- 2）数据装载
insert overwrite table ads_alarm_stat_last_month
select *
from ads_alarm_stat_last_month
union
select vin,
       '2023-05',
       sum(alarm_count),
       sum(l1_alarm_count),
       sum(l2_alarm_count),
       sum(l3_alarm_count)
from dws_car_alarm_1d
where substr(dt, 0, 7) = '2023-05'
group by vin;