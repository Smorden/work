with dim_sku as (
    select sku, department_3_name
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
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
    select dt, sku
             ,SUM(if(stock_stage = '海外仓在库', NVL(stock_amt, 0), 0)) as oversea_stock_amt
    from dws.dws_ivct_sc_asset_management_ds
    where dt between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
    group by dt, sku
    )
, oversea_future_instock as (
    select stock_date as dt, sku, sum(today_inbound_qty * theoretical_purchase_price) as today_inbound_amt
    from dws.dws_ivct_sku_site_future_oversea_stock_ds
    where dt = curdate()
            and stock_date between curdate()
        and last_day(date_add(curdate(), interval 1 month))
    group by stock_date, sku
    )
, profit_product_cost as (
    select dt, sku, sum(product_cost_cny_fi) as profit_product_cost
    from dws.dws_alct_theory_profit_sum_order_di
    where dt between date_sub(curdate(), interval 60 day) and date_sub(curdate(), interval 1 day)
    group by dt, sku
    )
, result as (
    select dt, sku
        , sum(sales_out_amt) as sales_out_amt
        , sum(oversea_stock_amt) as oversea_stock_amt
        , sum(today_inbound_amt) as today_inbound_amt
        , sum(profit_product_cost) as profit_product_cost
    from
    (
        select
            dt
          , sku
          , sales_out_amt
          , 0 oversea_stock_amt
          , 0 today_inbound_amt
          , 0 profit_product_cost
        from sales_out_amt
        union all
        select
            dt
          , sku
          , 0 sales_out_amt
          , oversea_stock_amt
          , 0 today_inbound_amt
          , 0 profit_product_cost
        from oversea_stock_amt
        union all
        select
            dt
          , sku
          , 0 sales_out_amt
          , 0 oversea_stock_amt
          , today_inbound_amt
          , 0 profit_product_cost
        from oversea_future_instock
        union all
        select
            dt
          , sku
          , 0 sales_out_amt
          , 0 oversea_stock_amt
          , 0 today_inbound_amt
          , profit_product_cost
        from profit_product_cost
        ) as t
    group by dt, sku
    )
select department_3_name 部门, dt 日期
    , sum(sales_out_amt) as 出库采购成本
    , sum(oversea_stock_amt) as 海外库存成本
    , sum(today_inbound_amt) as 预计入库成本
    , round(sum(profit_product_cost), 2) as 销售商品成本
from result as res
join dim_sku as sku on res.sku = sku.sku
where department_3_name not in ('','产品部')
group by department_3_name, dt
order by department_3_name, dt
;
