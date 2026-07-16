with dim_sku as (
    select sku
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
      and sku_tag like '%新品测款%'
    )
select po_order_code 采购单, sku, ps_supplier_name 供应商, purchase_price 采购价
                           , theoretical_purchase_price 初始采购价
                           , dt 初始采购价日期
from
    (
        select
            po.po_order_code
          , po.sku
          , po.ps_supplier_name
          , po.purchase_price
          , ss.theoretical_purchase_price
          , ss.dt
          , row_number() over (partition by po.sku order by ss.dt) as rn
        from
            dwd.dwd_fact_pcct_purchase_order_line_df as po
            join dim_sku                             as sku
                on sku.sku = po.sku
            left join dwd.dwd_dim_supplier_sku_ds         as ss
                on ss.sku = po.sku and ss.ps_supplier_id = po.ps_supplier_id and ss.dt >= '2023-08-26'
        where not (po.po_order_line_status in (30,70) and po.received_qty = 0)
        ) as t
where t.rn = 1
order by dt
;