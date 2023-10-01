-- 混动模式行驶日志事实表
drop table if exists dwd_car_running_hybrid_inc;
create external table dwd_car_running_hybrid_inc
(
    `vin`                             string comment '汽车唯一ID',
    `velocity`                        int comment '车速',
    `mileage`                         int comment '里程',
    `voltage`                         int comment '总电压',
    `electric_current`                int comment '总电流',
    `soc`                             int comment 'SOC',
    `dc_status`                       int comment 'DC-DC状态',
    `gear`                            int comment '挡位',
    `insulation_resistance`           int comment '绝缘电阻',
    `motor_count`                     int comment '驱动电机个数',
    `motor_list`                      array<struct<`id` :int, `status` :int, `rev` :int, `torque` :int,
                                                   `controller_temperature` :int, `temperature` :int, `voltage`
                                                   :int, `electric_current` :int>> comment '驱动电机列表',
    `fuel_cell_dc_status`             int comment '高压DC-DC状态',
    `engine_status`                   int comment '发动机状态',
    `crankshaft_speed`                int comment '曲轴转速',
    `fuel_consume_rate`               int comment '燃料消耗率',
    `max_voltage_battery_pack_id`     int comment '最高电压电池子系统号',
    `max_voltage_battery_id`          int comment '最高电压电池单体代号',
    `max_voltage`                     int comment '电池单体电压最高值',
    `min_temperature_subsystem_id`    int comment '最低电压电池子系统号',
    `min_voltage_battery_id`          int comment '最低电压电池单体代号',
    `min_voltage`                     int comment '电池单体电压最低值',
    `max_temperature_subsystem_id`    int comment '最高温度子系统号',
    `max_temperature_probe_id`        int comment '最高温度探针号',
    `max_temperature`                 int comment '最高温度值',
    `min_voltage_battery_pack_id`     int comment '最低温度子系统号',
    `min_temperature_probe_id`        int comment '最低温度探针号',
    `min_temperature`                 int comment '最低温度值',
    `battery_count`                   int comment '单体电池总数',
    `battery_pack_count`              int comment '单体电池包总数',
    `battery_voltages`                array<int> comment '单体电池电压值列表',
    `battery_temperature_probe_count` int comment '单体电池温度探针总数',
    `battery_pack_temperature_count`  int comment '单体电池包总数',
    `battery_temperatures`            array<int> comment '单体电池温度值列表',
    `timestamp`                       bigint comment '日志采集时间'
)
    comment '混动模式行驶日志事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dwd/dwd_car_running_hybrid_inc'
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
-- 1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table car_data.dwd_car_running_hybrid_inc partition (dt)
select `vin`,
       `velocity`,
       `mileage`,
       `voltage`,
       `electric_current`,
       `soc`,
       `dc_status`,
       `gear`,
       `insulation_resistance`,
       `motor_count`,
       `motor_list`,
       `fuel_cell_dc_status`,
       `engine_status`,
       `crankshaft_speed`,
       `fuel_consume_rate`,
       `max_voltage_battery_pack_id`,
       `max_voltage_battery_id`,
       `max_voltage`,
       `min_temperature_subsystem_id`,
       `min_voltage_battery_id`,
       `min_voltage`,
       `max_temperature_subsystem_id`,
       `max_temperature_probe_id`,
       `max_temperature`,
       `min_voltage_battery_pack_id`,
       `min_temperature_probe_id`,
       `min_temperature`,
       `battery_count`,
       `battery_pack_count`,
       `battery_voltages`,
       `battery_temperature_probe_count`,
       `battery_pack_temperature_count`,
       `battery_temperatures`,
       `timestamp`,
       `dt`
from car_data.ods_car_data_inc
where dt <= '2023-05-02'
  and car_status = 1
  and execution_mode = 2;
-- 2）每日装载
insert overwrite table car_data.dwd_car_running_hybrid_inc partition (dt = '2023-05-03')
select `vin`,
       `velocity`,
       `mileage`,
       `voltage`,
       `electric_current`,
       `soc`,
       `dc_status`,
       `gear`,
       `insulation_resistance`,
       `motor_count`,
       `motor_list`,
       `fuel_cell_dc_status`,
       `engine_status`,
       `crankshaft_speed`,
       `fuel_consume_rate`,
       `max_voltage_battery_pack_id`,
       `max_voltage_battery_id`,
       `max_voltage`,
       `min_temperature_subsystem_id`,
       `min_voltage_battery_id`,
       `min_voltage`,
       `max_temperature_subsystem_id`,
       `max_temperature_probe_id`,
       `max_temperature`,
       `min_voltage_battery_pack_id`,
       `min_temperature_probe_id`,
       `min_temperature`,
       `battery_count`,
       `battery_pack_count`,
       `battery_voltages`,
       `battery_temperature_probe_count`,
       `battery_pack_temperature_count`,
       `battery_temperatures`,
       `timestamp`
from car_data.ods_car_data_inc
where dt = '2023-05-03'
  and car_status = 1
  and execution_mode = 2;

