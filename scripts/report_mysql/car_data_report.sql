CREATE DATABASE IF NOT EXISTS car_data_report DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci;
# 11.1.2 创建表
# 1）里程相关统计
drop table if exists ads_mileage_stat_last_month;
create table ads_mileage_stat_last_month
(
    vin          varchar(20) comment '汽车唯一ID',
    mon          varchar(7) comment '统计月份',
    avg_mileage  int comment '日均里程',
    avg_speed    decimal(16, 2) comment '平均时速分子',
    danger_count decimal(16, 2) comment '平均百公里急加减速次数'
) comment '里程相关统计';

# 2）告警相关统计
drop table if exists ads_alarm_stat_last_month;
create table ads_alarm_stat_last_month
(
    vin            varchar(20) comment '汽车唯一ID',
    mon            varchar(7) comment '统计月份',
    alarm_count    int comment '告警次数',
    l1_alarm_count int comment '一级告警次数',
    l2_alarm_count int comment '二级告警次数',
    l3_alarm_count int comment '三级告警次数'
) comment '告警相关统计';

# 3）温控相关统计
drop table if exists ads_temperature_stat_last_month;
create table ads_temperature_stat_last_month
(
    vin                                varchar(20) comment '汽车唯一ID',
    mon                                varchar(7) comment '统计月份',
    max_motor_temperature              int comment '电机最高温度',
    avg_motor_temperature              decimal(16, 2) comment '电机平均温度',
    max_motor_controller_temperature   int comment '电机控制器最高温度',
    avg_motor_controller_temperature   decimal(16, 2) comment '电机控制器平均温度',
    max_battery_temperature            int comment '最高电池温度',
    battery_temperature_abnormal_count int comment '电池温度异常值次数'
) comment '温控相关统计';

# 4）能耗相关统计
drop table if exists ads_consume_stat_last_month;
create table ads_consume_stat_last_month
(
    vin                 varchar(20) comment '汽车唯一ID',
    mon                 varchar(7) comment '统计月份',
    soc_per_charge      decimal(16, 2) comment '次均充电电量',
    duration_per_charge decimal(16, 2) comment '次均充电时长',
    charge_count        int comment '充电次数',
    fast_charge_count   int comment '快充次数',
    slow_charge_count   int comment '慢充次数',
    fully_charge_count  int comment '深度充电次数',
    soc_per_100km       decimal(16, 2) comment 'soc百公里平均消耗',
    soc_per_run         decimal(16, 2) comment '每次里程soc平均消耗',
    soc_last_100km      decimal(16, 2) comment '最近百公里soc消耗'
) comment '能耗主题统计';