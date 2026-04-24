/*
# 需求
1. 近 3–6 个月 TK 销售占比变化
2. 新品上线 30 天出单率
3. 60 天持续出单率
4. 按类目统计：销量分组区间占比
5. 刊登时库存
6. 爆单后补货风险案例
# 输出
- 销售额
	- 月份，TK销售额，总销售额，占比
- 明细
	- sku，站点，首次刊登日期，首次出单日期，首次出单天数
	- 维度补充：pm，op，部门，中文名
	- 一级类目，二级类目，产品等级，日均平滑销量，销量分组
	- 刊登库存，首次采购数量
	- 新品测算销量，爆单日期，爆单日7天日均销量，爆单日4个月后的平滑销量
*/
-- SQL
-- 销售额
select
date_format(dt, '%Y年%m月') as 年月
, sum(if(platform_id = 31, sales_amt_cny_fi, 0)) as TK销售额
, sum(sales_amt_cny_fi) as 总销售额
, sum(if(platform_id = 31, sales_amt_cny_fi, 0)) / sum(sales_amt_cny_fi) as TK销售额占比
from ads.ads_alct_theory_profit_account_details_di
where dt between date_sub(curdate(), interval 6 month) and date_sub(curdate(), interval 1 day)
group by date_format(dt, '%Y年%m月')
order by date_format(dt, '%Y年%m月')
;
-- 明细
with min_list_dt as(
    select tl.sku, tl.site_id
        , max(tl.site_name) as site_name
        , date(date_add(min(tp.product_create_time_utc), interval 8 hour)) as min_list_dt
    from dwd.dwd_dim_opct_tiktok_listing_ds as tl
    join ods.ods_pbbs_api_tiktok_products_df as tp on tl.tiktok_product_id = tp.id
    group by tl.sku, tl.site_id
)
, site_stock as (
    select ws.dt, ws.sku, wh.warehouse_site_id, sum(stock_qty) as stock_qty
    from (
        select dt, sku, warehouse_id, stock_qty
        from dws.dws_ivct_goods_warehouse_stock_ds
        where dt >= '2024-05-01'
            and stock_stage = '海外仓在库'
        ) as ws
    join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = ws.warehouse_id
    group by ws.dt, ws.sku, wh.warehouse_site_id
)
, sales_channel as(
    select sku, site_id, sale_status, channel_user_name, charge_user_name, department_3_name
    from dwd.dwd_dim_sales_channel_ds
    where dt = date_sub(curdate(), interval 1 day)
        and platform_id = 31
)
, min_sale_dt as(
    select sku, site_id, min(dt) as min_sale_dt
    from dws.dws_alct_theory_profit_sku_plat_site_di
    where platform_id = 31 and sales_qty > 0
    group by sku, site_id
)
, dim_sku as (
    select sku, sku_name, lv1_category_name, lv2_category_name
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
)
, ss_range as (
    select 0 as range_start union all
    select 1 as range_start union all
    select 5 as range_start union all
    select 10 as range_start union all
    select 50 as range_start union all
    select 100 as range_start
)
, ss_range_full as(
    select range_start
        , lead(range_start, 1, 999999999) as range_end
    from ss_range
)
, smooth_sales as(
    select sku, site_id, clean_qty_smooth
    from dws.dws_alct_clean_smooth_sales_replenishment_ds
    where dt = date_sub(curdate(), interval 1 day)
        and platform_id = 31
)

;