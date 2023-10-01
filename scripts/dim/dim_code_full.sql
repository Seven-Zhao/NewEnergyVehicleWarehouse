-- 日志编码表
drop table if exists dim_code_full;
create external table dim_code_full
(
    `type`      string comment '编码类型',
    `code_id`   string comment '编码ID',
    `code_name` string comment '编码名称'
) comment '日志编码维度表'
    stored as orc
    location '/warehouse/car_data/dim/dim_code_full'
    tblproperties ('orc.compress' = 'snappy');