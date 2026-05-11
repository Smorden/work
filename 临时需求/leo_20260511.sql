create table if not exists temp.temp_leo_sku_site_df (
sku varchar(100)
, site_id int
) engine = olap
unique key(sku,site_id)
distributed by hash(sku,site_id) buckets 1
properties (
"light_schema_change" = "true"
);
insert into temp.temp_leo_sku_site_df values
('EZ1184231A1',2),
('AP250401001',2),
('EZ1170994MM001',2),
('AJ2516102003',2),
('3XHUISSGD001',2),
('AJ245504001',2),
('120X80CMZHEYP80',2),
('AJ2529901001',2),
('EZ1184714A1',2),
('AJ2414501002',2),
('120X80CMZHEYP88',2),
('EZ1184391A1',2),
('EZ1170994A1',2),
('AJ2530101001',2),
('AJ2414505001',2),
('AJ2524602001',2),
('AJ2530207001',2),
('AN254501001',2),
('AJ257501001',2),
('AJ2516102002',2)
;
select *
from temp.temp_leo_sku_site_df
;
/**
2. 补货 / 采购链路数据：
需求单日期, 需求数量, 采购单日期, 采购数量, 供应商, 固定流程天数,
采购处理天数, 供货期, 中仓处理天数, 头程天数, 供应商实际发货/交货日期,
中仓入库日期, 头程发运日期, 海外仓上架日期, 实际上架数量, 实际流程天数

3. 预测来源数据：系统自动预测销量, OP 最终采用预测销量,
最终采用方式：系统 / 人工, 人工预测生效日期, 人工预测有效期,
当时使用的季节性系数, 补货计算引用的预测日期/月份, 最终补货数量

根据附件的SKU+平台+站点数据，补充 2024-10-01 至 2026-04-30 的完整数据，一个样本一个EXCEL，每个模块一个SHEET。

 */
with dim_supplier as (
    select supplier_id, supplier_name
    from dwd.dwd_dim_supplier_ds
    where dt = date_sub(curdate(), interval 1  day)
)
, process_days as (
    select dt, process_days, sku, dc_code
        ,pricing_need_day,delivery_period,interval_warehouse_deal_day
        ,move_stock_days
    from dws.dws_ivct_sku_dc_process_days_ds
    where dt between '2024-10-01' and '2026-04-30'
        and sku in(
            select distinct sku from temp.temp_leo_sku_site_df
        )
)
, result as (
select
    pb.sku
    , pb.batch_code
    , pb.demand_bill_code
    , pb.demand_date
    , pb.demand_qty
    , if(pb.po_order_date='1970-01-01', null, pb.po_order_date) as po_order_date
    , pb.purchase_qty
    , sp.supplier_name
    , pd.process_days
    , pd.pricing_need_day
    , pd.delivery_period
    , pd.interval_warehouse_deal_day
    , pd.move_stock_days
    , pb.fixed_last_sign_date
    , if(date(pb.transfer_in_time)='1970-01-01', null, date(pb.transfer_in_time)) as tranfer_in_date
    , transfer_in_qty
    , if(date(pb.move_in_time)='1970-01-01', null, date(pb.move_in_time)) as move_in_date
    , move_in_qty
    , if(date(pb.oversea_in_time)='1970-01-01', null, date(pb.oversea_in_time)) as oversea_in_date
    , pb.oversea_in_qty
    , datediff(if(date(pb.oversea_in_time)='1970-01-01', null, date(pb.oversea_in_time))
        , pb.demand_date) as actual_process_days
from dws.dws_ivct_supplier_chain_full_process_batch_df as pb
join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = pb.demand_warehouse_id
join temp.temp_leo_sku_site_df as sku on sku.sku = pb.sku and sku.site_id = wh.warehouse_site_id
join dim_supplier as sp on sp.supplier_id = pb.supplier_id
left join process_days as pd on pd.dt = pb.demand_date and pd.sku = pb.sku and pd.dc_code = wh.dc_code
where demand_date between '2024-10-01' and '2026-04-30'
)
select
sku
, batch_code as 批次号
, demand_bill_code as 需求单号
, demand_date as 需求单日期
, demand_qty as 需求数量
, po_order_date as 采购单日期
, purchase_qty as 采购数量
, supplier_name as 供应商
, process_days as 固定流程天数
, pricing_need_day as 采购处理天数
, delivery_period as 供货期
, interval_warehouse_deal_day as 中仓处理天数
, move_stock_days as 头程天数
, fixed_last_sign_date as 签收日期
, tranfer_in_date as 中仓入库日期
, transfer_in_qty as 中仓入库数量
, move_in_date 头程发运日期
, move_in_qty 头程发运数量
, oversea_in_date 海外仓上架日期
, oversea_in_qty 实际上架数量
, actual_process_days as 实际流程天数
from result
order by sku, demand_date
;
/**
3. 预测来源数据：系统自动预测销量, OP 最终采用预测销量,
最终采用方式：系统 / 人工, 人工预测生效日期, 人工预测有效期,
当时使用的季节性系数, 补货计算引用的预测日期/月份, 最终补货数量
 */
with demand_bill as (
    select dm.sku, dm.demand_date, dm.demand_qty, dm.bill_code
    from dwd.dwd_fact_pcct_purchase_demand_bill_df as dm
    join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = dm.demand_warehouse_id
    join temp.temp_leo_sku_site_df as sku on sku.sku = dm.sku and sku.site_id = wh.warehouse_site_id
    where dm.demand_date between '2024-10-01' and '2026-04-30'
)
, rp_sales as (
    SELECT
        a.dt, a.sku, a.platform_name, a.site_name
        , a.qty_smooth as daily_sales_sys, a.daily_sales
        ,case when a.daily_sales_decide ='sys' then '系统'
        when a.daily_sales_decide in ('manual_long','manual_period') then '人工'
        when a.confirm_forecast_sales = a.qty_smooth then '系统'
        else '人工' end daily_sales_decide
        ,a.daily_sales_decide as origin_decide
        ,a.seasonal_coefficient
        ,a.sale_status
    FROM dwd.`dwd_fact_opct_forecast_sales_process_di` as a
    join temp.temp_leo_sku_site_df as sku on sku.sku = a.sku and sku.site_id = a.site_id
    where a.dt between '2024-10-01' and '2026-04-30'
)
select
    db.sku
    , db.demand_date as 需求单日期
    , db.bill_code as 需求单号
    , db.demand_qty as 需求数量
    , rs.platform_name as 平台
    , rs.sale_status as 产品等级
    , rs.daily_sales_sys as 系统预测销量
    , rs.daily_sales as 最终采用预测销量
    , rs.daily_sales_decide as 最终采用方式
    , rs.seasonal_coefficient as 当时使用的季节性系数
from demand_bill as db
left join rp_sales as rs on db.sku = rs.sku and db.demand_date = rs.dt
    and (rs.sale_status = '在售' or rs.daily_sales > 0)
order by db.sku, db.demand_date, db.bill_code, rs.platform_name
;