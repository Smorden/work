-- SKU编码、中文名称、报关中文名称、报关英文名称、单位、申报要素
-- 、出口HS编码、退税限制、境内货源地、国家、进口HS编码
with sku_tag as(
    select
       distinct a.sku
    from ods.ods_lh_doc_oc_operation_tag_relation_df a
         inner join ods.ods_lh_doc_oc_operation_tag_df b
            on a.tag_code = b.tag_code
            and b.record_status = 1
         inner join ods.ods_lh_doc_oc_operation_tag_used_relation_df as c
            on c.system_module_code = 'tag_used_system_module_pc'
            and c.tag_code = b.tag_code
            and c.record_status = 1
    where a.record_status = 1
    and b.tag_name = '退税限制'
    )
select
    a.SKU编码, c.sku_name as 中文名称, 报关中文名称, 报关英文名称, 单位, 申报要素, 出口HS编码
     , if(d.sku is not null, '是', '否') as 退税限制, 境内货源地
, b.country_code 国家, b.import_hs_code 进口HS编码
from
    (
        select
            data_code                            as SKU编码
          , replace(declare_cn_name,'|||', '%')                      as 报关中文名称
          , replace(declare_en_name,'|||', '%')                      as 报关英文名称
          , unit                                 as 单位
          , replace(declare_element, '|||', '%') as 申报要素
          , export_hs_code                       as 出口HS编码
          , domestic_goods_place                 as 境内货源地
        from ods.ods_lh_doc_oc_product_customs_info_df
        where
              data_type = 'SKU'
          and record_status = 1
-- and data_code = 'CR2620083001'
        ) as a
left join (
    select data_code, country_code, import_hs_code
    from ods.ods_lh_doc_oc_product_tariff_df
    where data_type  = 'SKU'
      and record_status = 1
        ) as b on b.data_code = a.SKU编码
join (
    select sku, replace(sku_name, '|||', '%') as sku_name
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
        and sku_status = '在售'
        ) as c on c.sku = a.SKU编码
left join sku_tag as d on d.sku = a.SKU编码
where b.country_code = 'US'
order by a.SKU编码, b.country_code
;
select *
from dwd.dwd_dim_sku_tag_df
where tag_name = '退税限制'
;
show create table ods.ods_lh_doc_oc_product_customs_info_df
;

returnRateLimitFlag

;
show create table ods.ods_lh_doc_oc_product_tariff_df
;
CREATE TABLE `ods_lh_doc_oc_product_tariff_df` (
                                                   `id` varchar(2000) NULL DEFAULT "",
                                                   `data_code` text NULL DEFAULT "" COMMENT 'spu/sku',
                                                   `data_type` text NULL DEFAULT "" COMMENT '数据类型(sku、spu)',
                                                   `country_code` text NULL DEFAULT "" COMMENT '国家二字码',
                                                   `tariff_rate` text NULL DEFAULT "" COMMENT '关税税率',
                                                   `import_value` text NULL DEFAULT "" COMMENT '进口货值(USD)',
                                                   `import_hs_code` text NULL DEFAULT "" COMMENT '进口HS编码',
                                                   `ts_check_number` text NULL DEFAULT "" COMMENT 'ts查验次数',
                                                   `create_time` text NULL DEFAULT "" COMMENT '创建时间',
                                                   `update_time` text NULL DEFAULT "" COMMENT '修改时间',
                                                   `record_status` text NULL DEFAULT "" COMMENT '1表示正常，0表示删除',
                                                   `create_user_id` text NULL DEFAULT "" COMMENT '创建人',
                                                   `update_user_id` text NULL DEFAULT "" COMMENT '更新人',
                                                   `version_num` text NULL DEFAULT "" COMMENT '数据版本，每次更新+1'
) ENGINE=OLAP
    DUPLICATE KEY(`id`)
COMMENT 'spu/sku关税表'
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);
CREATE TABLE `ods_lh_doc_oc_product_customs_info_df` (
                                                         `id` varchar(2000) NULL DEFAULT "",
                                                         `data_code` text NULL DEFAULT "" COMMENT 'spu/sku',
                                                         `data_type` text NULL DEFAULT "" COMMENT '数据类型(sku、spu)',
                                                         `declare_cn_name` text NULL DEFAULT "" COMMENT '报关中文名称',
                                                         `declare_en_name` text NULL DEFAULT "" COMMENT '报关英文名称',
                                                         `unit` text NULL DEFAULT "" COMMENT '单位',
                                                         `declare_element` text NULL DEFAULT "" COMMENT '申报要素',
                                                         `export_hs_code` text NULL DEFAULT "" COMMENT '出口HS编码',
                                                         `product_inspect` text NULL DEFAULT "" COMMENT '是否商检(0:否1:是)',
                                                         `fumigable` text NULL DEFAULT "" COMMENT '是否熏蒸(0:否1:是)',
                                                         `export_value` text NULL DEFAULT "" COMMENT '出口货值',
                                                         `origin_country_code` text NULL DEFAULT "" COMMENT '原产国',
                                                         `domestic_goods_place` text NULL DEFAULT "" COMMENT '境内货源地',
                                                         `added_tax_refund_rate` text NULL DEFAULT "" COMMENT '增值税退税率(出口退税需求用到),保留百分比后一位小数',
                                                         `create_time` text NULL DEFAULT "" COMMENT '创建时间',
                                                         `update_time` text NULL DEFAULT "" COMMENT '修改时间',
                                                         `record_status` text NULL DEFAULT "" COMMENT '1表示正常，0表示删除',
                                                         `create_user_id` text NULL DEFAULT "" COMMENT '创建人',
                                                         `update_user_id` text NULL DEFAULT "" COMMENT '更新人',
                                                         `version_num` text NULL DEFAULT "" COMMENT '数据版本，每次更新+1'
) ENGINE=OLAP
    DUPLICATE KEY(`id`)
COMMENT 'spu/sku关务信息表'
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);