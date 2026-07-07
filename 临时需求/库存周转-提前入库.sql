with
    dim_sku as (
        select sku, department_3_name from dwd.dwd_dim_sku_ds where dt = date_sub(curdate(), interval 1 day)
        )
  , sales_out_amt as (
        select
            dt
          , sku
          , sum(sales_out_amt) as sales_out_amt
        from dws.dws_alct_sales_out_details_di
        where
            dt between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
        group by
            dt
          , sku
        )
  , oversea_stock_amt as (
        select
            dt
          , sku
          , SUM(if(stock_stage = '海外仓在库', NVL(stock_amt, 0), 0)) as oversea_stock_amt
        from dws.dws_ivct_sc_asset_management_ds
        where
            dt between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
        group by
            dt
          , sku
        )
,  stock_in_order as (
    select dt, sku, order_num, order_num_origin, qty, warehouse_id
    from dwd.dwd_fact_ivct_ic_stock_in_order_di
    where dt between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
        and stock_order_type_name = '调拨入库'
    )
   , advance_inbound_amt as (
       select si.dt, si.sku, sum(si.qty * sku.theoretical_purchase_price) as advance_inbound_amt
       from stock_in_order as si
       join dwd.dwd_dim_ivct_ship_info_ds as sh on sh.ship_no = si.order_num_origin and sh.dt = si.dt
       join dwd.dwd_dim_sku_ds as sku on si.sku = sku.sku and si.dt = sku.dt
       where sh.estimate_inbound_date >= '2026-07-01'
       group by si.dt, si.sku
    )
  , result as (
        select
            dt
          , sku
          , sum(sales_out_amt)       as sales_out_amt
          , sum(oversea_stock_amt)   as oversea_stock_amt
          , sum(advance_inbound_amt)   as advance_inbound_amt
        from
            (
                select
                    dt
                  , sku
                  , sales_out_amt
                  , 0 oversea_stock_amt
                  , 0 advance_inbound_amt
                from sales_out_amt
                union all
                select
                    dt
                  , sku
                  , 0 sales_out_amt
                  , oversea_stock_amt
                  , 0 advance_inbound_amt
                from oversea_stock_amt
                union all
                select dt, sku, 0 sales_out_amt, 0 oversea_stock_amt, advance_inbound_amt
                from advance_inbound_amt
                ) as t
        group by
            dt
          , sku
        )
select
    dt                                    日期
, department_3_name                     部门
  , sum(sales_out_amt)                 as 当日出库金额
  , sum(oversea_stock_amt)             as 当日海外仓库存金额
, sum(advance_inbound_amt)             as 当日提前入库金额
from
    result       as res
    join dim_sku as sku
        on res.sku = sku.sku
where
    department_3_name not in ('', '产品部')
group by
    department_3_name
  , dt
order by
    dt, department_3_name
;
select count(distinct order_num)
from dwd.dwd_fact_ivct_ic_stock_in_order_di
where dt between date_sub(curdate(), interval 1 day) and date_sub(curdate(), interval 1 day)
  and stock_order_type_name = '调拨入库'
;