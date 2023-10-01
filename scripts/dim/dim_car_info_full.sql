drop table if exists dim_car_info_full;
create external table dim_car_info_full
(
    `id`               string comment '车辆唯一编码',
    `type_id`          string comment '车型ID',
    `type`             string comment '车型',
    `sale_type`        string comment '销售车型',
    `trademark`        string comment '品牌',
    `company`          string comment '厂商',
    `seating_capacity` int comment '准载人数',
    `power_type`       string comment '车辆动力类型',
    `charge_type`      string comment '车辆支持充电类型',
    `category`         string comment '车辆分类',
    `weight_kg`        int comment '总质量（kg）',
    `warranty`         string comment '整车质保期（年/万公里）'
) comment '车辆信息维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/car_data/dim/dim_car_info_full'
    tblproperties ('orc.compress' = 'snappy');