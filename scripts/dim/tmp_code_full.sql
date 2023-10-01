-- 创建临时表
drop table if exists tmp_code_full;
create external table tmp_code_full
(
    `type`      string comment '编码类型',
    `code_id`   string comment '编码ID',
    `code_name` string comment '编码名称'
) comment '日志编码维度表'
    row format delimited
        fields terminated by '\t'
    location '/warehouse/car_data/tmp/tmp_code_full';

-- 创建好之后将car_data_code.txt 直接上传到'/warehouse/car_data/tmp/tmp_code_full'目录中即可
-- 然后insert 到维度表dim_code_full
insert into table dim_code_full select * from tmp_code_full;