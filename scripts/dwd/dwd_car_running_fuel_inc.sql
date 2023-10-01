-- 8.3 新能源燃料模式行驶日志事实表
-- 8.3.1 建表语句
Drop table if exists dwd_car_running_fuel_inc;
create external table dwd_car_running_fuel_inc
(
    `vin`                                         string comment '汽车唯一ID',
    `velocity`                                    int comment '车速',
    `mileage`                                     int comment '里程',
    `voltage`                                     int comment '总电压',
    `electric_current`                            int comment '总电流',
    `soc`                                         int comment 'SOC',
    `dc_status`                                   int comment 'DC-DC状态',
    `gear`                                        int comment '挡位',
    `insulation_resistance`                       int comment '绝缘电阻',
    `fuel_cell_voltage`                           int comment '燃料电池电压',
    `fuel_cell_current`                           int comment '燃料电池电流',
    `fuel_cell_consume_rate`                      int comment '燃料消耗率',
    `fuel_cell_temperature_probe_count`           int comment '燃料电池温度探针总数',
    `fuel_cell_temperature`                       int comment '燃料电池温度值',
    `fuel_cell_max_temperature`                   int comment '氢系统中最高温度',
    `fuel_cell_max_temperature_probe_id`          int comment '氢系统中最高温度探针号',
    `fuel_cell_max_hydrogen_consistency`          int comment '氢气最高浓度',
    `fuel_cell_max_hydrogen_consistency_probe_id` int comment '氢气最高浓度传感器代号',
    `fuel_cell_max_hydrogen_pressure`             int comment '氢气最高压力',
    `fuel_cell_max_hydrogen_pressure_probe_id`    int comment '氢气最高压力传感器代号',
    `fuel_cell_dc_status`                         int comment '高压DC-DC状态',
    `engine_status`                               int comment '发动机状态',
    `crankshaft_speed`                            int comment '曲轴转速',
    `fuel_consume_rate`                           int comment '燃料消耗率',
    `max_voltage_battery_pack_id`                 int comment '最高电压电池子系统号',
    `max_voltage_battery_id`                      int comment '最高电压电池单体代号',
    `max_voltage`                                 int comment '电池单体电压最高值',
    `min_temperature_subsystem_id`                int comment '最低电压电池子系统号',
    `min_voltage_battery_id`                      int comment '最低电压电池单体代号',
    `min_voltage`                                 int comment '电池单体电压最低值',
    `max_temperature_subsystem_id`                int comment '最高温度子系统号',
    `max_temperature_probe_id`                    int comment '最高温度探针号',
    `max_temperature`                             int comment '最高温度值',
    `min_voltage_battery_pack_id`                 int comment '最低温度子系统号',
    `min_temperature_probe_id`                    int comment '最低温度探针号',
    `min_temperature`                             int comment '最低温度值',
    `timestamp`                                   bigint comment '日志采集时间'
)
    comment '新能源燃料模式行驶日志事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dwd/dwd_car_running_fuel_inc'
    tblproperties ('orc.compress' = 'snappy');

-- 8.3.2 数据装载
-- 1）首日装载
insert overwrite table dwd_car_running_fuel_inc partition (dt)
select `vin`,
       `velocity`,
       `mileage`,
       `voltage`,
       `electric_current`,
       `soc`,
       `dc_status`,
       `gear`,
       `insulation_resistance`,
       `fuel_cell_voltage`,
       `fuel_cell_current`,
       `fuel_cell_consume_rate`,
       `fuel_cell_temperature_probe_count`,
       `fuel_cell_temperature`,
       `fuel_cell_max_temperature`,
       `fuel_cell_max_temperature_probe_id`,
       `fuel_cell_max_hydrogen_consistency`,
       `fuel_cell_max_hydrogen_consistency_probe_id`,
       `fuel_cell_max_hydrogen_pressure`,
       `fuel_cell_max_hydrogen_pressure_probe_id`,
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
       `timestamp`,
       dt
from ods_car_data_inc
where dt <= '2023-05-02'
  and car_status = 1
  and execution_mode = 3;

-- 2）每日装载
insert overwrite table dwd_car_running_fuel_inc partition (dt = '2023-05-03')
select `vin`,
       `velocity`,
       `mileage`,
       `voltage`,
       `electric_current`,
       `soc`,
       `dc_status`,
       `gear`,
       `insulation_resistance`,
       `fuel_cell_voltage`,
       `fuel_cell_current`,
       `fuel_cell_consume_rate`,
       `fuel_cell_temperature_probe_count`,
       `fuel_cell_temperature`,
       `fuel_cell_max_temperature`,
       `fuel_cell_max_temperature_probe_id`,
       `fuel_cell_max_hydrogen_consistency`,
       `fuel_cell_max_hydrogen_consistency_probe_id`,
       `fuel_cell_max_hydrogen_pressure`,
       `fuel_cell_max_hydrogen_pressure_probe_id`,
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
       `timestamp`
from ods_car_data_inc
where dt = '2023-05-03'
  and car_status = 1
  and execution_mode = 3;