
select ss.sku, ss.supplier_name as 默认供应商
    , ss.sku_price as 采购价
    -- , ss.factory_sku_price as 不含运采购价
    -- , ss.default_supplier_flag as 默认供应商标记
    , sku.sku_name as sku名称
    , sku.create_time as 创建时间
from dwd.dwd_dim_supplier_sku_ds as ss
join (select sku,sku_name,create_time from dwd.dwd_dim_sku_ds where dt = date_sub(curdate(), interval 1 day) and sku_status = '在售') as sku on sku.sku = ss.sku
where ss.dt = date_sub(curdate(), interval 1 day)
and ss.enable_status = 1
and ss.default_supplier_flag = 1
order by ss.sku, ss.supplier_name
;
