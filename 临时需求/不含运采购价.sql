
select ss.sku, supplier_name as 供应商
    , sku_price as 采购价
    , factory_sku_price as 不含运采购价
    , default_supplier_flag as 默认供应商标记
from dwd.dwd_dim_supplier_sku_ds as ss
where dt = date_sub(curdate(), interval 1 day)
and enable_status = 1
order by ss.sku, supplier_name
;