-- 9.2.3 汽车电机信息汇总
-- 1）建表语句
drop table if exists dws_car_motor_1d;
create table dws_car_motor_1d
(
    vin                              string comment '汽车唯一ID',
    max_motor_temperature            int comment '电机最高温度',
    avg_motor_temperature            decimal(16, 2) comment '电机平均温度',
    max_motor_controller_temperature int comment '电机控制器最高温度',
    avg_motor_controller_temperature decimal(16, 2) comment '电机控制器平均温度'
)
    comment '汽车电机单日累计表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dws/dws_car_motor_1d'
    tblproperties ('orc.compress' = 'snappy');
-- 2）数据装载
-- （1）首日装载
insert overwrite table dws_car_motor_1d partition (dt)
select vin,
       max(temperature)            max_motor_temperature,
       avg(temperature)            avg_motor_temperature,
       max(controller_temperature) max_motor_controller_temperature,
       avg(controller_temperature) avg_motor_controller_temperature,
       dt
from dwd_car_motor_inc
where dt <= '2023-05-02'
group by dt, vin;
-- （2）每日装载
insert overwrite table dws_car_motor_1d partition (dt = '2023-05-03')
select vin,
       max(temperature)            max_motor_temperature,
       avg(temperature)            avg_motor_temperature,
       max(controller_temperature) max_motor_controller_temperature,
       avg(controller_temperature) avg_motor_controller_temperature
from dwd_car_motor_inc
where dt = '2023-05-03'
group by vin;