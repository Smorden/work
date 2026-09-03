select ps_supplier_name 供应商, sku, sku_name sku名称
from db_purchase_execute.pe_supplier_tax_goods
where record_status = 1 and enable_status = 1
order by id
;
with sku_tag as(
    select
        distinct a.sku
    from ods.ods_lh_doc_oc_operation_tag_relation_df a
         inner join ods.ods_lh_doc_oc_operation_tag_df b
            on a.tag_code = b.tag_code
            and b.record_status = 1
            and b.tag_name = '退税限制'
         inner join ods.ods_lh_doc_oc_operation_tag_used_relation_df as c
            on c.system_module_code = 'tag_used_system_module_pc'
            and c.tag_code = b.tag_code
            and c.record_status = 1
    where a.record_status = 1
    )
select
    c.sku, c.sku_name as 中文名称, c.sku_status
from(
        select sku, replace(sku_name, '|||', '%') as sku_name, sku_status
        from dwd.dwd_dim_sku_ds
        where dt = date_sub(curdate(), interval 1 day)
        ) as c
    join sku_tag as d on d.sku = c.sku
order by c.SKU
;