
select
    sku.sku
,sku.weight 重量
,sku.length 长
,sku.width 宽
,sku.height 高
,fs.site_name 站点
,ifnull(fs.daily_sales, 0)*30 as 月补货销量
from
    (
        select
            sku
          , weight
          , length
          , width
          , height
        from dwd.dwd_dim_sku_ds
        where
              dt = date_sub(curdate(), interval 1 day)
          and sku_status = '在售'
        ) as sku
join (
    select sku, site_name, sum(daily_sales) as daily_sales
    from dwd.dwd_fact_opct_forecast_sales_process_di
    where dt = date_sub(curdate(), interval 1 day)
    and site_name in ('DE','UK')
    and sale_status = '在售'
    group by sku, site_name
        ) as fs on fs.sku = sku.sku
order by sku.sku, fs.site_name
;
select count(1)
from
    (
        select
            sku
          , site_name
          , sum(daily_sales) as daily_sales
        from dwd.dwd_fact_opct_forecast_sales_process_di
        where
              dt = date_sub(curdate(), interval 1 day)
          and site_name in ('DE', 'UK')
          and sale_status = '在售'
        group by
            sku
          , site_name
        ) as t
;