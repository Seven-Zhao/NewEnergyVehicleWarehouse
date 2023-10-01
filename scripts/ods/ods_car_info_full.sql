-- 汽车信息全量表
drop table if exists ods_car_info_full;
create external table ods_car_info_full
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
) comment '整车信息表'
    partitioned by (`dt` string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/car_data/ods/ods_car_info_full';

-- hive 里面默认的空值null为"\N"， DataX同步过来的数据默认的空值是 ''
-- 可以在建表的时候加上 null defined as '' ，就能识别到空值。