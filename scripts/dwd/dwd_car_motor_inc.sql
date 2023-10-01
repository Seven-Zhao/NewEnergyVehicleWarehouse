-- 8.7 驱动电机日志事实表
-- 8.7.1 建表语句
drop table if exists dwd_car_motor_inc;
create external table dwd_car_motor_inc
(
    `vin`                    string comment '汽车唯一ID',
    `id`                     int comment '电机ID',
    `status`                 int comment '电机状态',
    `rev`                    int comment '电机转速',
    `torque`                 int comment '电机转矩',
    `controller_temperature` int comment '电机控制器温度',
    `temperature`            int comment '电机温度',
    `voltage`                int comment '电机控制器输入电压',
    `electric_current`       int comment '电机控制器直流母线电流',
    `timestamp`              bigint comment '日志采集时间'
)
    comment '驱动电机日志事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dwd/dwd_car_motor_inc'
    tblproperties ('orc.compress' = 'snappy');

-- 8.7.2 数据装载
-- 1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_car_motor_inc partition (dt)
select vin,
       motor.id,
       motor.status,
       motor.rev,
       motor.torque,
       motor.controller_temperature,
       motor.temperature,
       motor.voltage,
       motor.electric_current,
       `timestamp`,
       dt
from ods_car_data_inc
         lateral view explode(motor_list) tmp as motor
where dt <= '2023-05-02';
-- 2）每日装载
insert overwrite table dwd_car_motor_inc partition (dt = '2023-05-03')
select vin,
       motor.id,
       motor.status,
       motor.rev,
       motor.torque,
       motor.controller_temperature,
       motor.temperature,
       motor.voltage,
       motor.electric_current,
       `timestamp`
from ods_car_data_inc
         lateral view explode(motor_list) tmp as motor
where dt = '2023-05-03';