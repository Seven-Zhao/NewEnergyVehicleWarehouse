#!/bin/bash

APP='car_data'
if [ -n "$2" ]; then
  do_date=$2
else
  do_date=$(date -d '-1 day' +%F)
fi

dwd_car_running_electricity_inc="insert overwrite table ${APP}.dwd_car_running_electricity_inc partition(dt='${do_date}')
select
    vin,
    velocity,
    mileage,
    voltage,
    electric_current,
    soc,
    dc_status,
    gear,
    insulation_resistance,
    motor_count,
    motor_list,
    fuel_cell_dc_status,
    engine_status,
    crankshaft_speed,
    fuel_consume_rate  ,
    max_voltage_battery_pack_id,
    max_voltage_battery_id,
    max_voltage,
    min_temperature_subsystem_id,
    min_voltage_battery_id,
    min_voltage,
    max_temperature_subsystem_id,
    max_temperature_probe_id,
    max_temperature,
    min_voltage_battery_pack_id,
    min_temperature_probe_id,
    min_temperature,
    battery_count,
    battery_pack_count,
    battery_voltages,
    battery_temperature_probe_count,
    battery_pack_temperature_count,
    battery_temperatures,
    \`timestamp\`
from ${APP}.ods_car_data_inc
where dt='${do_date}'
and car_status=1
and execution_mode=1;
"

dwd_car_running_hybrid_inc="insert overwrite table ${APP}.dwd_car_running_hybrid_inc partition(dt='${do_date}')
select
    vin,
    velocity,
    mileage,
    voltage,
    electric_current,
    soc,
    dc_status,
    gear,
    insulation_resistance,
    motor_count,
    motor_list,
    fuel_cell_dc_status,
    engine_status,
    crankshaft_speed,
    fuel_consume_rate  ,
    max_voltage_battery_pack_id,
    max_voltage_battery_id,
    max_voltage,
    min_temperature_subsystem_id,
    min_voltage_battery_id,
    min_voltage,
    max_temperature_subsystem_id,
    max_temperature_probe_id,
    max_temperature,
    min_voltage_battery_pack_id,
    min_temperature_probe_id,
    min_temperature,
    battery_count,
    battery_pack_count,
    battery_voltages,
    battery_temperature_probe_count,
    battery_pack_temperature_count,
    battery_temperatures,
    \`timestamp\`
from ${APP}.ods_car_data_inc
where dt='${do_date}'
and car_status=1
and execution_mode=2;
"

dwd_car_running_fuel_inc="insert overwrite table ${APP}.dwd_car_running_fuel_inc partition(dt='${do_date}')
select
    vin,
    velocity,
    mileage,
    voltage,
    electric_current,
    soc,
    dc_status,
    gear,
    insulation_resistance,
    fuel_cell_voltage,
    fuel_cell_current,
    fuel_cell_consume_rate,
    fuel_cell_temperature_probe_count,
    fuel_cell_temperature,
    fuel_cell_max_temperature,
    fuel_cell_max_temperature_probe_id,
    fuel_cell_max_hydrogen_consistency,
    fuel_cell_max_hydrogen_consistency_probe_id,
    fuel_cell_max_hydrogen_pressure,
    fuel_cell_max_hydrogen_pressure_probe_id,
    fuel_cell_dc_status,
    engine_status,
    crankshaft_speed,
    fuel_consume_rate,
    max_voltage_battery_pack_id,
    max_voltage_battery_id,
    max_voltage,
    min_temperature_subsystem_id,
    min_voltage_battery_id,
    min_voltage,
    max_temperature_subsystem_id,
    max_temperature_probe_id,
    max_temperature,
    min_voltage_battery_pack_id,
    min_temperature_probe_id,
    min_temperature,
    \`timestamp\`
from ${APP}.ods_car_data_inc
where dt='${do_date}'
and car_status=1
and execution_mode=3;
"

dwd_car_parking_charging_inc="insert overwrite table ${APP}.dwd_car_parking_charging_inc partition(dt='${do_date}')
select
    vin,
    voltage,
    electric_current,
    soc,
    dc_status,
    gear,
    insulation_resistance,
    engine_status,
    crankshaft_speed,
    fuel_consume_rate,
    max_voltage_battery_pack_id,
    max_voltage_battery_id,
    max_voltage,
    min_temperature_subsystem_id,
    min_voltage_battery_id,
    min_voltage,
    max_temperature_subsystem_id,
    max_temperature_probe_id,
    max_temperature,
    min_voltage_battery_pack_id,
    min_temperature_probe_id,
    min_temperature,
    battery_count,
    battery_pack_count,
    battery_voltages,
    battery_temperature_probe_count,
    battery_pack_temperature_count,
    battery_temperatures,
    \`timestamp\`
from ${APP}.ods_car_data_inc
where dt='${do_date}'
and car_status=2
and charge_status=1;
"

dwd_car_running_charging_inc="insert overwrite table ${APP}.dwd_car_running_charging_inc partition(dt='${do_date}')
select
    vin,
    velocity,
    mileage,
    voltage,
    electric_current,
    soc,
    dc_status,
    gear,
    insulation_resistance,
    motor_count,
    motor_list,
    fuel_cell_voltage,
    fuel_cell_current,
    fuel_cell_consume_rate,
    fuel_cell_temperature_probe_count,
    fuel_cell_temperature,
    fuel_cell_max_temperature,
    fuel_cell_max_temperature_probe_id,
    fuel_cell_max_hydrogen_consistency,
    fuel_cell_max_hydrogen_consistency_probe_id,
    fuel_cell_max_hydrogen_pressure,
    fuel_cell_max_hydrogen_pressure_probe_id,
    fuel_cell_dc_status,
    engine_status,
    crankshaft_speed,
    fuel_consume_rate,
    max_voltage_battery_pack_id,
    max_voltage_battery_id,
    max_voltage,
    min_temperature_subsystem_id,
    min_voltage_battery_id,
    min_voltage,
    max_temperature_subsystem_id,
    max_temperature_probe_id,
    max_temperature,
    min_voltage_battery_pack_id,
    min_temperature_probe_id,
    min_temperature,
    battery_count,
    battery_pack_count,
    battery_voltages,
    battery_temperature_probe_count,
    battery_pack_temperature_count,
    battery_temperatures,
    \`timestamp\`
from ${APP}.ods_car_data_inc
where dt='${do_date}'
and car_status=1
and charge_status=2;
"

dwd_car_alarm_inc="insert overwrite table ${APP}.dwd_car_alarm_inc partition(dt='${do_date}')
select
    vin,
    car_status,
    charge_status,
    execution_mode,
    velocity,
    mileage,
    voltage,
    electric_current,
    soc,
    dc_status,
    gear,
    insulation_resistance,
    motor_count,
    motor_list,
    fuel_cell_voltage,
    fuel_cell_current,
    fuel_cell_consume_rate,
    fuel_cell_temperature_probe_count,
    fuel_cell_temperature,
    fuel_cell_max_temperature,
    fuel_cell_max_temperature_probe_id,
    fuel_cell_max_hydrogen_consistency,
    fuel_cell_max_hydrogen_consistency_probe_id,
    fuel_cell_max_hydrogen_pressure,
    fuel_cell_max_hydrogen_pressure_probe_id,
    fuel_cell_dc_status,
    engine_status,
    crankshaft_speed,
    fuel_consume_rate,
    max_voltage_battery_pack_id,
    max_voltage_battery_id,
    max_voltage,
    min_temperature_subsystem_id,
    min_voltage_battery_id,
    min_voltage,
    max_temperature_subsystem_id,
    max_temperature_probe_id,
    max_temperature,
    min_voltage_battery_pack_id,
    min_temperature_probe_id,
    min_temperature,
    alarm_level,
    alarm_sign,
    custom_battery_alarm_count,
    custom_battery_alarm_list,
    custom_motor_alarm_count,
    custom_motor_alarm_list,
    custom_engine_alarm_count,
    custom_engine_alarm_list,
    other_alarm_count,
    other_alarm_list,
    battery_count,
    battery_pack_count,
    battery_voltages,
    battery_temperature_probe_count,
    battery_pack_temperature_count,
    battery_temperatures,
    \`timestamp\`
from ${APP}.ods_car_data_inc
where dt='${do_date}'
and alarm_level>0;
"
dwd_car_motor_inc="insert overwrite table ${APP}.dwd_car_motor_inc partition(dt='${do_date}')
select
    vin,
    motor.id ,
    motor.status,
    motor.rev,
    motor.torque,
    motor.controller_temperature,
    motor.temperature,
    motor.voltage,
    motor.electric_current,
    \`timestamp\`
from ${APP}.ods_car_data_inc
lateral view explode(motor_list) tmp as motor
where dt<='${do_date}';
"
case $1 in
'dwd_car_running_electricity_inc')
  hive -e "$dwd_car_running_electricity_inc"
  ;;
'dwd_car_running_hybrid_inc')
  hive -e "$dwd_car_running_hybrid_inc"
  ;;
'dwd_car_running_fuel_inc')
  hive -e "$dwd_car_running_fuel_inc"
  ;;
'dwd_car_parking_charging_inc')
  hive -e "$dwd_car_parking_charging_inc"
  ;;
'dwd_car_running_charging_inc')
  hive -e "$dwd_car_running_charging_inc"
  ;;
'dwd_car_alarm_inc')
  hive -e "$dwd_car_alarm_inc"
  ;;
'dwd_car_motor_inc')
  hive -e "$dwd_car_motor_inc"
  ;;

"all")
  hive -e "$dwd_car_running_electricity_inc$dwd_car_running_hybrid_inc$dwd_car_running_fuel_inc$dwd_car_parking_charging_inc$dwd_car_running_charging_inc$dwd_car_alarm_inc$dwd_car_motor_inc"
  ;;
esac
