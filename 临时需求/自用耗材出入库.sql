with dim_sku as (
    select sku, sku_name
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
      and lv1_category_name = '自用耗材'
    )
select *
from
    (
        select
            '入库'                出入库
          , dt                    日期
          , a.sku
          , goods_code            货品
          , sku_name              名称
          , qty                   数量
          , stock_order_type_name 单据类型
        from
            dwd.dwd_fact_ivct_ic_stock_in_order_di as a
            join dim_sku                           as b
                on b.sku = a.sku
        where
              dt between '2024-01-01' and '2026-07-31'
          and record_status = 1
        union all
        select
            '出库'                出入库
          , dt                    日期
          , a.sku
          , goods_code            货品
          , sku_name              名称
          , qty                   数量
          , stock_order_type_name 单据类型
        from
            dwd.dwd_fact_ivct_ic_stock_out_order_di as a
            join dim_sku                            as b
                on b.sku = a.sku
        where
              dt between '2024-01-01' and '2026-07-31'
          and record_status = 1
        ) as t
order by 日期,出入库
;