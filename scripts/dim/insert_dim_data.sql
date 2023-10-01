-- DIM层数据装载
insert overwrite table car_data.dim_car_info_full partition (dt = '2023-05-02')
select id,
       type_id,
       type,
       sale_type,
       trademark,
       company,
       seating_capacity,
       power_type,
       charge_type,
       category,
       weight_kg,
       warranty
from car_data.ods_car_info_full o
where o.dt = '2023-05-02';