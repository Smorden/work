/*
 ods_lh_dfc_fc_standard_oms_accounting_cost_df
ods_lh_dfc_fc_standard_oms_sale_fees_df
ods_lh_dfc_fc_standard_oms_sale_income_df
ods_lh_dfc_fc_standard_oms_sale_promotion_df
 ods_lh_dfc_fc_standard_oms_sale_cost_df
ods_lh_dfc_fc_standard_oms_sale_fees_item_di
 问题：
 1、fc_standard_oms_accounting_cost表里没有standard_type_code='SALES_COST_AcrossMonthsRED'的数据
 2、
 */
 select *
 from
     (
         select * from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
                  where standard_type_code = 'SALES_COST_UnshippedForeAccounting'
         ) as a
 left join (
         select *
         from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
         where standard_type_code = 'SALES_COST_AcrossMonthsRED'
         ) as b on b.order_no = a.order_no and b.account_id = a.account_id
 where b.order_no is null
     ;
select date_trunc(origin_order_date, 'month')
     ,standard_type_code, sum(cast(amount as double)), count(1)
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
group by date_trunc(origin_order_date, 'month'),standard_type_code
order by date_trunc(origin_order_date, 'month'),standard_type_code
;
select count(1), count(distinct origin_num)
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
where standard_type_code = 'OMS_AfterSales_COSTRED'
;
select a.*
from
    (
        select *
        from dwd.dwd_dim_opct_oms_aftersale_orders_df
        where
              date(check_time) between '2026-02-01' and '2026-04-30'
          and order_type = '退款'
          and order_statusid = 2
          and refund_type = '发货前退款'
        ) as a
left join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        ) as b on b.origin_num = a.ref_number
where b.origin_num is null
order by a.check_time desc
;
select a.dt, a.account_id, a.order_id, a.sku , a.out_qty
           , a.product_cost_cny_fi
           , b.amount, b.qty
from
    (
        select dt, account_id, order_id, sku, max(out_qty) as out_qty
        ,sum(product_cost_cny_fi) as product_cost_cny_fi
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
        and product_cost_cny_fi < 0
        group by dt, account_id, order_id, sku
        ) as a
    join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        ) as b on b.account_id = a.account_id and b.order_no = a.order_id
        -- and b.order_line_item_id = a.order_line_itemid
        and b.sku = a.sku
where a.product_cost_cny_fi <> b.amount
order by a.dt, a.account_id, a.order_id
;
select *
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
where order_no = 'PO-210-11183913359991531'
;
select *
from ods.ods_lh_dfc_fc_standard_oms_sale_fees_item_di
where  order_no = 'PO-210-11183913359991531'
;
select *
from dwd.dwd_fact_alct_theory_profit_details_di
where order_id = 'PO-210-11183913359991531'
;
select *
from dws.dws_alct_theory_profit_refund_cost_backwash_df
where order_id = 'PO-210-11183913359991531'
;
select * from dwd.dwd_fact_tcct_oms_refund_df
where order_id = 'PO-210-11183913359991531'
;
select*
from ods.ods_pbbs_oc_so_after_sales_order_df
where original_order_no = 'PO-210-11183913359991531'
;
select payment_or_refund_amont
from dwd.dwd_dim_tcct_oms_orders_di
where order_id = 'PO-210-11183913359991531'
;
select *
from dws.dws_alct_theory_profit_sum_order_di
where order_id = 'PO-210-11183913359991531'
;
select a.dt, a.account_id, a.order_id, a.order_line_itemid, a.sku , a.out_qty
, a.product_cost_cny_fi
, b.amount
, c.parcel_id, c.dt, c.status_info
from
    (
        select *
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
        ) as a
    left join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        ) as b on b.account_id = a.account_id and b.order_no = a.order_id
        -- and b.order_line_item_id = a.order_line_itemid
        and b.sku = a.sku
left join dwm.dwm_tcct_oms_parcel_detail_di as c on c.order_id = a.order_id
                                                        and c.account_id = a.account_id
and c.order_line_itemid = a.order_line_itemid
and c.sku = a.sku
where b.account_id is null and c.parcel_id is null
order by a.dt
;
-- 2026-03-05	qinghan1301	302-8189227-3855527	302-8189227-3855527-106489497	EZ1174706A1
select *
from dws.dws_alct_refund_order_df
where order_id = '11-13986-57752'
;
select *
from dws.dws_alct_theory_profit_refund_cost_backwash_df
where
      dt between '2026-01-28' and '2026-01-31'
  and out_qty = 0
;
select dt, parcel_id, order_id, account_id, order_line_itemid, sku, status_info
from dwm.dwm_tcct_oms_parcel_detail_di
where order_id = '11-13986-57752'
;

select  b.account_id, b.order_no, b.origin_num, b.sku , b.qty
           , b.amount
           , c.parcel_id, c.dt, c.status_info
from
    (
        select *
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
        ) as a
    right join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        ) as b on b.account_id = a.account_id and b.order_no = a.order_id
        -- and b.order_line_item_id = a.order_line_itemid
        and b.sku = a.sku
    left join dwm.dwm_tcct_oms_parcel_detail_di as c on c.order_id = b.order_no
        and c.account_id = b.account_id
        and c.sku = b.sku
where a.account_id is null
and ifnull(c.status_info, '') = '妥投完成'
;
-- eBay_roomsandbloom	27-14133-66435	SH260304001040	EZ1167546A1	2
select *
from dws.dws_alct_refund_order_df
where order_id = '27-14133-66435'
;
select *
from dwd.dwd_fact_tcct_oms_refund_df
where order_id = '11-13986-57752'
;
select cancel_type_code, paid_time, order_source, order_status_name
from dwd.dwd_dim_tcct_oms_orders_di
where order_id = '11-13986-57752'
;
-- Bonsami	PO-211-05383461284473273	SH260308000284	AX244601001	1	-80.4700	FH177163235844675649	2026-02-21	妥投完成
-- puzhengltd	205-9630597-0076326	SH260303000508	SG+GEF	1	-62.3700	FH177187455490425884	2026-02-24	妥投完成

select *
from dwm.dwm_tcct_sales_order_fulfillment_sku_details_di
where order_id = '205-9630597-0076326'
;
select *
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
where order_no  = '205-9630597-0076326'
;