
select ss.sku, ss.supplier_name as 默认供应商
    , ss.theoretical_purchase_price as 采购价
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
-- 所有采购价
select    a.sku, sku_name sku名称
               , supplier_name 供应商
               , theoretical_purchase_price as `含运采购价(无税)`
  -- , theoretical_purchase_price_with_tax as `含运采购价(含税)`
               , factory_theoretical_purchase_price as `不含运采购价(无税)`
  -- , factory_theoretical_purchase_price_with_tax as `不含运采购价(含税)`
               , if(default_supplier_flag=1,'是','否') 是否默认供应商
from
    (
        select
            sku
          , supplier_name
          , theoretical_purchase_price
          , factory_theoretical_purchase_price
          , theoretical_purchase_price_with_tax
          , factory_theoretical_purchase_price_with_tax
          , default_supplier_flag
        from dwd.dwd_dim_supplier_sku_ds
        where
              dt = date_sub(curdate(), interval 1 day)
          and enable_status = 1
        ) as a
    join (
        select sku, sku_name
        from dwd.dwd_dim_sku_ds
        where dt = date_sub(curdate(), interval 1 day)
        ) as b on b.sku = a.sku
order by sku, default_supplier_flag desc
;