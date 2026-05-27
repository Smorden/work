/*
ods_lh_dfc_fc_standard_oms_accounting_cost_di

ods_lh_dfc_fc_standard_oms_sale_fees_di
ods_lh_dfc_fc_standard_oms_sale_income_di
ods_lh_dfc_fc_standard_oms_sale_promotion_di
ods_lh_dfc_fc_standard_oms_sale_cost_di

ods_lh_dfc_fc_standard_oms_sale_fees_item_di

ods_pbbs_oc_so_order_financial_accounting_list_di

so_order_financial_accounting_skus
 */
select *
from
    (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where
            standard_type_code = 'SALES_COST_UnshippedForeAccounting'
        ) as a
    left join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'SALES_COST_AcrossMonthsRED'
        ) as b
        on b.order_no = a.order_no and b.account_id = a.account_id
where
    b.order_no is null
;
select
    date_trunc(origin_order_date, 'month')
  , standard_type_code
  , sum(cast(amount as double))
  , count(1)
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
group by
    date_trunc(origin_order_date, 'month')
  , standard_type_code
order by
    date_trunc(origin_order_date, 'month')
  , standard_type_code
;
select
    count(1)
  , count(distinct origin_num)
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
where
    standard_type_code = 'OMS_AfterSales_COSTRED'
;
select
    a.*
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
        ) as b
        on b.origin_num = a.ref_number
where
    b.origin_num is null
order by
    a.check_time desc
;
select
    a.dt
  , a.account_id
  , a.order_id
  , a.sku
  , a.out_qty
  , a.product_cost_cny_fi
  , b.amount
  , b.qty
from
    (
        select
            dt
          , account_id
          , order_id
          , sku
          , max(out_qty)             as out_qty
          , sum(product_cost_cny_fi) as product_cost_cny_fi
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
          and product_cost_cny_fi < 0
        group by
            dt
          , account_id
          , order_id
          , sku
        ) as a
    join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        ) as b
        on b.account_id = a.account_id and b.order_no = a.order_id
        -- and b.order_line_item_id = a.order_line_itemid
        and b.sku = a.sku
where
    a.product_cost_cny_fi <> b.amount
order by
    a.dt
  , a.account_id
  , a.order_id
;
select *
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
where
    order_no = '02-14184-97955'
;
;
select *
from ods.ods_lh_dfc_fc_standard_oms_sale_fees_item_di
where
    order_no = 'PO-210-11183913359991531'
;
select *
from dwd.dwd_fact_alct_theory_profit_details_di
where
    order_id = 'PO-210-11183913359991531'
;
select *
from dws.dws_alct_theory_profit_refund_cost_backwash_df
where
    order_id = 'PO-210-11183913359991531'
;
select *
from dwd.dwd_fact_tcct_oms_refund_df
where
    order_id = 'PO-210-11183913359991531'
;
select*
from ods.ods_pbbs_oc_so_after_sales_order_df
where
    original_order_no = 'PO-210-11183913359991531'
;
select
    payment_or_refund_amont
from dwd.dwd_dim_tcct_oms_orders_di
where
    order_id = 'PO-210-11183913359991531'
;
select *
from dws.dws_alct_theory_profit_sum_order_di
where
    order_id = 'PO-210-11183913359991531'
;
select
    a.dt
  , a.account_id
  , a.order_id
  , a.order_line_itemid
  , a.sku
  , a.out_qty
  , a.product_cost_cny_fi
  , b.amount
  , c.parcel_id
  , c.dt
  , c.status_info
from
    (
        select *
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
        )                                       as a
    left join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        )                                       as b
        on b.account_id = a.account_id and b.order_no = a.order_id
        -- and b.order_line_item_id = a.order_line_itemid
        and b.sku = a.sku
    left join dwm.dwm_tcct_oms_parcel_detail_di as c
        on c.order_id = a.order_id and c.account_id = a.account_id and c.order_line_itemid = a.order_line_itemid and
           c.sku = a.sku
where
      b.account_id is null
  and c.parcel_id is null
order by
    a.dt
;
-- 2026-03-05	qinghan1301	302-8189227-3855527	302-8189227-3855527-106489497	EZ1174706A1
select *
from dws.dws_alct_refund_order_df
where
    order_id = '11-13986-57752'
;
select *
from dws.dws_alct_theory_profit_refund_cost_backwash_df
where
      dt between '2026-01-28' and '2026-01-31'
  and out_qty = 0
;
select
    dt
  , parcel_id
  , order_id
  , account_id
  , order_line_itemid
  , sku
  , status_info
from dwm.dwm_tcct_oms_parcel_detail_di
where
    order_id = '11-13986-57752'
;

select
    b.account_id
  , b.order_no
  , b.origin_num
  , b.sku
  , b.qty
  , b.amount
  , c.parcel_id
  , c.dt
  , c.status_info
from
    (
        select *
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
        )                                       as a
    right join (
        select *
        from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
        where standard_type_code = 'OMS_AfterSales_COSTRED'
        )                                       as b
        on b.account_id = a.account_id and b.order_no = a.order_id
        -- and b.order_line_item_id = a.order_line_itemid
        and b.sku = a.sku
    left join dwm.dwm_tcct_oms_parcel_detail_di as c
        on c.order_id = b.order_no and c.account_id = b.account_id and c.sku = b.sku
where
      a.account_id is null
  and ifnull(c.status_info, '') = '妥投完成'
;
-- eBay_roomsandbloom	27-14133-66435	SH260304001040	EZ1167546A1	2
select *
from dws.dws_alct_refund_order_df
where
    order_id = '27-14133-66435'
;
select *
from dwd.dwd_fact_tcct_oms_refund_df
where
    order_id = '11-13986-57752'
;
select
    cancel_type_code
  , paid_time
  , order_source
  , order_status_name
from dwd.dwd_dim_tcct_oms_orders_di
where
    order_id = '11-13986-57752'
;
-- Bonsami	PO-211-05383461284473273	SH260308000284	AX244601001	1	-80.4700	FH177163235844675649	2026-02-21	妥投完成
-- puzhengltd	205-9630597-0076326	SH260303000508	SG+GEF	1	-62.3700	FH177187455490425884	2026-02-24	妥投完成

select *
from dwm.dwm_tcct_sales_order_fulfillment_sku_details_di
where
    order_id = '205-9630597-0076326'
;
select *
from ods.ods_lh_dfc_fc_standard_oms_accounting_cost_df
where
    order_no = '205-9630597-0076326'
;

-- 退款冲收入
with
    base as (
        select*
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
                where
                    standard_type_code = 'OMS_AfterSales_InComeRED'
                ) as t
        where
              rn = 1
          and record_status = 1
        )
  , big_data as (
        select *
        from dws.dws_alct_refund_order_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and cancel_date is null
          and refund_or_resend_amount > 0
        )
  , accounting_order as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_pbbs_oc_so_order_financial_accounting_list_di
                ) as t
        where
            rn = 1
        )
select
    bg.check_time
  , bg.ref_number
  , bg.account_id
  , bg.order_id
  , bg.paid_time
  , bg.refund_or_resend_amount
  , bs.origin_num
  , bs.amount
  , ao.order_no
from
    big_data                   as bg
    left join base             as bs
        on bg.ref_number = bs.origin_num
    left join accounting_order as ao
        on bg.order_id = ao.order_no
where
    bs.origin_num is null
-- and ao.order_no is null
order by
    bg.check_time desc
  , bg.ref_number desc
;
select *
from ods.ods_pbbs_oc_so_order_financial_accounting_list_di
where
    order_no = '576830944260365055'
;

-- 退款冲收入
with
    base as (
        select*
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
                where
                    standard_type_code = 'OMS_AfterSales_InComeRED'
                ) as t
        where
              rn = 1
          and record_status = 1
        )
  , big_data as (
        select *
        from dws.dws_alct_refund_order_df
        where dt between '2026-02-01' and '2026-04-30' and cancel_date is null
        )
select
    bs.account_id
  , bs.order_no
  , bs.origin_num
  , bs.amount
  , od.paid_time
  , od.order_source
  , od.order_status_name
  , od.cancel_type_code
from
    big_data                            as bg
    right join base                     as bs
        on bg.ref_number = bs.origin_num
    join dwd.dwd_dim_tcct_oms_orders_di as od
        on od.order_id = bs.order_no and od.account_id = bs.account_id
where
      bg.ref_number is null
  and od.cancel_type_code = 'INVALID'
order by
    bg.check_time desc
  , bg.ref_number desc
;
select *
from dwd.dwd_fact_tcct_oms_refund_df
where
    ref_number = 'SH260221001057'
;
select
    cancel_type_code
  , paid_time
  , order_source
  , order_status_name
from dwd.dwd_dim_tcct_oms_orders_di
where
    order_id = '07-14252-85137'
;

-- 退款冲收入
with
    base as (
        select*
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
                where
                    standard_type_code = 'OMS_AfterSales_InComeRED'
                ) as t
        where
              rn = 1
          and record_status = 1
        )
  , big_data as (
        select
            ref_number
          , sku
          , sum(refund_amount_orderline) as sku_refund_amt
        from dwd.dwd_fact_tcct_oms_refund_df
        where
              date(check_time) between '2026-02-01' and '2026-04-30'
          and cancel_date is null
          and refund_amount_orderline > 0
        group by
            ref_number
          , sku
        )
select
    bg.ref_number
  , bg.sku
  , bg.sku_refund_amt
  , bs.origin_num
  , bs.amount
from
    big_data  as bg
    join base as bs
        on bg.ref_number = bs.origin_num and bg.sku = bs.sku
where
    abs(bg.sku_refund_amt + bs.amount) > 0.01
order by
    bg.ref_number desc
;
select
    refund_amount_orderline
  , refund_or_resend_amount
from dwd.dwd_fact_tcct_oms_refund_df
where
    ref_number = 'SH260430002584'
;

-- 退款冲推广费 OMS_AfterSales_PromotionRED fc_standard_oms_sale_promotion
with
    base as (
        select
            account_id
          , order_no
          , sum(cast(amount as double)) as amount
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_promotion_di
                where
                    standard_type_code = 'OMS_AfterSales_PromotionRED'
                ) as t
        where
            rn = 1
        group by
            account_id
          , order_no
        )
  , bg_data as (
        select
            dt
          , ref_number
          , account_id
          , order_id
          , sku
          , sum(promotion_fee_cny_fi) as promotion_fee_cny_fi
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and promotion_fee_cny_fi <> 0
        group by
            dt
          , ref_number
          , account_id
          , order_id
          , sku
        )
  , accounting_order as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_pbbs_oc_so_order_financial_accounting_list_di
                ) as t
        where
            rn = 1
        )
  , refund_order as (
        select
            account_id
          , order_id
          , sum(refund_amount_orderline) as refund_amount_order
        from dwd.dwd_fact_tcct_oms_refund_df
        where
              cancel_date is null
          and date(check_time) between '2026-02-01' and '2026-04-30'
          and aftersale_order_status_id = 2
        group by
            account_id
          , order_id
        )
select
    bg.*
  , ro.refund_amount_order
  , od.amount
  , od.promotion_fee
  , bs.amount
from
    bg_data                                 as bg
    join dwd.dwd_fact_tcct_oms_refund_df    as rf
        on rf.ref_number = bg.ref_number and rf.aftersale_order_status_id = 2
    left join base                          as bs
        on bg.account_id = bs.account_id and bg.order_id = bs.order_no
    left join accounting_order              as ao
        on bg.order_id = ao.order_no and bg.account_id = ao.account_id
    join refund_order                       as ro
        on ro.order_id = bg.order_id and ro.account_id = bg.account_id
    join dwd.dwd_dim_tcct_oms_orders_fee_di as od
        on od.order_id = bg.order_id and od.account_id = bg.account_id
where
      ro.refund_amount_order >= od.amount
-- bs.account_id is null and ao.order_no is not null
  and abs(bs.amount + od.promotion_fee) > 0.1
order by
    bg.dt
  , bg.ref_number
  , bg.order_id
  , bg.account_id
  , bg.sku
;
select
    aftersale_order_status_id
  , cancel_date
from dwd.dwd_fact_tcct_oms_refund_df
where
    ref_number = 'SH260429001041'
;
select *
from dwd.dwd_dim_tcct_oms_orders_fee_di
where
    order_id = '026-7831746-3932359'
;
select
    sku
  , origin_num
from ods.ods_lh_dfc_fc_standard_oms_sale_promotion_di
where
      standard_type_code = 'OMS_AfterSales_PromotionRED'
  and order_no = 'PO-210-03065206497830048'
;
select
    account_id
  , order_id
  , sum(refund_amount_orderline) as refund_amount_order
from dwd.dwd_fact_tcct_oms_refund_df
group by
    account_id
  , order_id
;
select
    sales_amt_ori_total
from dws.dws_alct_refund_order_df
where
      dt between '2026-02-01' and '2026-04-30'
  and cancel_date is null
  and order_id = 'PO-076-06609641063031930'
;
select
    dt
  , promotion_fee_ori
  , company_holding_vat_ori
  , platform_charge_vat_ori
from dwd.dwd_fact_alct_theory_profit_details_di
where
    order_id = '026-7831746-3932359'
;
select *
from ods.ods_lh_dfc_fc_standard_oms_sale_promotion_di
where
    order_no = '026-7831746-3932359'
;
select
    standard_type_code
  , order_no
  , sku
  , amount
from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
where
    order_no = '026-7831746-3932359'
;
-- 退款冲推广费 OMS_AfterSales_PromotionRED fc_standard_oms_sale_promotion
with
    base as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_promotion_di
                where
                    standard_type_code = 'OMS_AfterSales_PromotionRED'
                ) as t
        where
            rn = 1
        )
  , bg_data as (
        select
            dt
          , max(ref_number)           as ref_number
          , account_id
          , order_id
          , sku
          , sum(promotion_fee_cny_fi) as promotion_fee_cny_fi
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and promotion_fee_cny_fi <> 0
        group by
            dt
          , account_id
          , order_id
          , sku
        )
select
    bs.origin_num
  , bs.sku
  , bs.account_id
  , bs.order_no
  , bs.order_paid_time
  , bs.origin_order_time
  , bs.amount
  , od.cancel_type_code
  , od.order_source
  , od.order_status_name
from
    bg_data                             as bg
    right join base                     as bs
        on bg.order_id = bs.order_no and bg.account_id = bs.account_id and bg.sku = bs.sku
    join dwd.dwd_dim_tcct_oms_orders_di as od
        on od.order_id = bs.order_no and od.account_id = bs.account_id
where
      bg.ref_number is null
  and od.order_source <> 1
;
select
    dt
  , promotion_fee_ori
  , sku
  , order_id
  , account_id
  , order_lineitem_id
  , etl_create_time
from dwd.dwd_fact_alct_theory_profit_details_di
where
    order_id = '026-0617367-1305955'
;
/*
SH260318000891	AW2510103003
SH260318000890	AW2510103001
SH260318000892	AW2510103002
 */
select
    promotion_fee_cny_fi
  , ref_number
  , sku
from dws.dws_alct_theory_profit_refund_cost_backwash_df
where
    order_id = '026-0617367-1305955'
;
select
    account_id
  , order_id
  , sku
  , order_lineitemid
  , etl_create_time
from dwd.dwd_fact_tcct_oms_refund_df
where
    order_id = '026-0617367-1305955'
;
select *
from ods.ods_pbbs_oc_so_after_sales_order_item_df
where
    order_no = '026-0617367-1305955'
;
-- 46691353 46939304
select *
from dwd.dwd_fact_tcct_oms_orders_lineitem_di
where
    oms_orders_lineitem_id = 46939304
;
select *
from dws.dws_alct_refund_order_df
where
    order_id = '026-0617367-1305955'
;
select
    origin_num
  , sku
  , amount
from
    (
        select *
             , row_number() over (partition by id order by dt desc) as rn
        from ods.ods_lh_dfc_fc_standard_oms_sale_promotion_di
        where
            standard_type_code = 'OMS_AfterSales_PromotionRED'
        ) as t
where
      rn = 1
  and order_no = '026-0617367-1305955'
;
-- 退款冲VAT	OMS_AfterSales_VATRED	fc_standard_oms_sale_income
with
    base as (
        select
            account_id
          , order_no
          , sum(cast(vat_amount as double)) as amount
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
                where
                    standard_type_code = 'OMS_AfterSales_VATRED'
                ) as t
        where
            rn = 1
        group by
            account_id
          , order_no
        )
  , refund_order as (
        select
            account_id
          , order_id
          , sum(refund_amount_orderline) as refund_amount_order
          , max(ref_number)              as ref_number
        from dwd.dwd_fact_tcct_oms_refund_df
        where
              cancel_date is null
          and date(check_time) between '2026-02-01' and '2026-04-30'
          and aftersale_order_status_id = 2
        group by
            account_id
          , order_id
        )
  , bg_data as (
        select distinct
            od.account_id
          , od.order_id
          , ro.ref_number
          , od.company_vat_fee
          , od.platform_vat_fee
          , od.company_vat_fee + od.platform_vat_fee as vat_fee
        from
            refund_order                            as ro
            join dwd.dwd_dim_tcct_oms_orders_fee_di as od
                on od.order_id = ro.order_id and od.account_id = ro.account_id and
                   od.company_vat_fee + od.platform_vat_fee > 0
        where
            ro.refund_amount_order >= od.amount
        )
  , accounting_order as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_pbbs_oc_so_order_financial_accounting_list_di
                ) as t
        where
            rn = 1
        )
select
    bg.*
from
    bg_data                    as bg
    right join base            as bs
        on bg.account_id = bs.account_id and bg.order_id = bs.order_no
    left join accounting_order as ao
        on bg.order_id = ao.order_no and bg.account_id = ao.account_id
where
-- bs.account_id is null and ao.order_no is not null
-- abs(bs.amount + bg.vat_fee) < 0.1
-- bg.order_id is null
order by
    bg.order_id
  , bg.account_id
;
select *
from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
where
      standard_type_code = 'OMS_AfterSales_VATRED'
  and order_no = 'PO-210-00174408504953473'
;
-- vat和推广费互斥
with
    base1 as (
        select
            account_id
          , order_no
          , sum(cast(vat_amount as double)) as amount
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_income_di
                where
                    standard_type_code = 'OMS_AfterSales_VATRED'
                ) as t
        where
            rn = 1
        group by
            account_id
          , order_no
        )
  , base2 as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_promotion_di
                where
                    standard_type_code = 'OMS_AfterSales_PromotionRED'
                ) as t
        where
            rn = 1
        )
select
    b1.order_no
from
    base1      as b1
    join base2 as b2
        on b1.account_id = b2.account_id and b1.order_no = b2.order_no
;

/*
 退款冲费用(发货后)	OMS_AfterSales_AfterFEERED	fc_standard_oms_sale_fees
 */
with
    base as (
        select
            t1.account_id
          , t1.order_no
          , sum(if(t2.fee_code = 'SZ004001', t2.amount * er.last_currency_rate, 0)) as platform_change_fee_cny_fi
          , sum(if(t2.fee_code = 'SZ004002', t2.amount * er.last_currency_rate, 0)) as platform_fixed_fee_cny_fi
          , sum(if(t2.fee_code = 'SZ004003', t2.amount * er.last_currency_rate, 0)) as international_fee_cny_fi
          , sum(if(t2.fee_code = 'SZ004004', t2.amount * er.last_currency_rate, 0)) as currency_conversion_charge_fee_cny_fi
          , sum(if(t2.fee_code = 'SZ004006', t2.amount * er.last_currency_rate, 0)) as paypal_fee_cny_fi
          , sum(if(t2.fee_code = 'SZ004021', t2.amount * er.last_currency_rate, 0)) as ebay_supervision_fee_cny_fi
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_fees_di
                where
                    standard_type_code = 'OMS_AfterSales_AfterFEERED'
                )                             as t1
            join (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_fees_item_di
                where
                    standard_type_code = 'OMS_AfterSales_AfterFEERED'
                )                             as t2
                on t2.standard_unique_num = t1.standard_unique_num
            join dwd.dwd_dim_exchange_rate_df as er
                on t1.currency_code = er.currency_code and
                   date(t1.order_paid_time) between er.start_date and er.end_date
        where
              t1.rn = 1
          and t2.rn = 1
        group by
            t1.account_id
          , t1.order_no
        )
  , refund_order as (
        select
            account_id
          , order_id
          , sum(refund_amount_orderline) as refund_amount_order
          , max(ref_number)              as ref_number
        from dwd.dwd_fact_tcct_oms_refund_df
        where
              cancel_date is null
          and date(check_time) between '2026-02-01' and '2026-04-30'
          and aftersale_order_status_id = 2
        group by
            account_id
          , order_id
        )
  , fr_order as (
        select distinct
            od.account_id
          , od.order_id
          , ro.ref_number
          , od.company_vat_fee
          , od.platform_vat_fee
          , od.company_vat_fee + od.platform_vat_fee as vat_fee
        from
            refund_order                            as ro
            join dwd.dwd_dim_tcct_oms_orders_fee_di as od
                on od.order_id = ro.order_id and od.account_id = ro.account_id
        where
            ro.refund_amount_order >= od.amount
        )
  , bg_data as (
        select
            account_id
          , order_id
          , sum(platform_change_fee_cny_fi)            as platform_change_fee_cny_fi
          , sum(platform_fixed_fee_cny_fi)             as platform_fixed_fee_cny_fi
          , sum(international_fee_cny_fi)              as international_fee_cny_fi
          , sum(currency_conversion_charge_fee_cny_fi) as currency_conversion_charge_fee_cny_fi
          , sum(paypal_fee_cny_fi)                     as paypal_fee_cny_fi
          , sum(ebay_supervision_fee_cny_fi)           as ebay_supervision_fee_cny_fi
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty > 0
        group by
            account_id
          , order_id
        having
            sum(platform_change_fee_cny_fi) + sum(platform_fixed_fee_cny_fi) + sum(international_fee_cny_fi) +
            sum(currency_conversion_charge_fee_cny_fi) + sum(paypal_fee_cny_fi) + sum(ebay_supervision_fee_cny_fi) <> 0
        )
  , accounting_order as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_pbbs_oc_so_order_financial_accounting_list_di
                ) as t
        where
            rn = 1
        )
select
   bg.*, bs.*
from
    bg_data                    as bg
    join fr_order              as fr
        on fr.order_id = bg.order_id and fr.account_id = bg.account_id
    left join base             as bs
        on bg.account_id = bs.account_id and bg.order_id = bs.order_no
    left join accounting_order as ao
        on bg.order_id = ao.order_no and bg.account_id = ao.account_id
where
      -- bs.account_id is null and ao.order_no is not null
      -- bg.order_id is null
abs(bs.platform_change_fee_cny_fi - bg.platform_change_fee_cny_fi) >= 1 or
abs(bs.platform_change_fee_cny_fi - bg.platform_change_fee_cny_fi) >= 1 or
abs(bs.platform_fixed_fee_cny_fi - bg.platform_fixed_fee_cny_fi) >= 1 or
abs(bs.international_fee_cny_fi - bg.international_fee_cny_fi) >= 1 or
abs(bs.currency_conversion_charge_fee_cny_fi - bg.currency_conversion_charge_fee_cny_fi) >= 1 or
abs(bs.paypal_fee_cny_fi - bg.paypal_fee_cny_fi) >= 1 or
abs(bs.ebay_supervision_fee_cny_fi - bg.ebay_supervision_fee_cny_fi) >= 1
order by abs(bs.platform_change_fee_cny_fi - bg.platform_change_fee_cny_fi) desc
;

select standard_unique_num
from ods.ods_lh_dfc_fc_standard_oms_sale_fees_di
where
      standard_type_code = 'OMS_AfterSales_AfterFEERED'
  and order_no = '576862288618559959'
;
SZ004001	-7.4300
SZ004002	-0.2900
SZ004004	-1.5400
SZ004021	-0.2400
;
select fee_code, amount
from ods.ods_lh_dfc_fc_standard_oms_sale_fees_item_di
where
      standard_type_code = 'OMS_AfterSales_AfterFEERED'
  and standard_unique_num =
      'OMS_AfterSales_AfterFEERED_TT_LixHomeGarden_SH260327003098_576862288618559959_CR251806001'
;
select
    income_expense_item_code
  , income_expense_item_name
from dwd.dwd_dim_income_expense_item_df
where
    income_expense_item_code in ('SZ004001',
                                 'SZ004002',
                                 'SZ004003',
                                 'SZ004004',
                                 'SZ004006',
                                 'SZ004021'
        )
order by
    income_expense_item_code
;

;
select
    platform_change_fee_cny_fi, out_qty, dt
from dws.dws_alct_theory_profit_refund_cost_backwash_df
where
    order_id = '576862288618559959'
;

;
select *
from dws.dws_alct_refund_order_df
where
    order_id = '576862288618559959'
;
select
    platform_change_fee_ori
  , dt
  , `platform_change_fee_currency_code`
from dwd.dwd_fact_alct_theory_profit_details_di
where
    order_id  = '576862288618559959'
;
select
    platform_change_fee_cny_fi
  , dt
from dws.dws_alct_theory_profit_sum_order_di
where
    order_id = '576862288618559959'
;
select
    currency_rate
  , last_currency_rate
  , current_currency_rate
, profit_currency_rate
from dwd.dwd_dim_exchange_rate_df
where
      currency_code = 'GBP'
  and '2025-12-24' between start_date and end_date
;

/*

退款冲费用-仓储费(发货前)	OMS_AfterSales_STORAGE_BeforeFEERED	fc_standard_oms_sale_fees
退款冲费用-尾程费(发货前)	OMS_AfterSales_TAIL_BeforeFEERED	fc_standard_oms_sale_fees

 */
with
    base as (
        select
            t1.account_id
          , t1.order_no
          , sum(if(t2.standard_type_code = 'OMS_AfterSales_STORAGE_BeforeFEERED', t2.amount * er.last_currency_rate, 0)) as stock_fee_cny_fi
          , sum(if(t2.standard_type_code = 'OMS_AfterSales_TAIL_BeforeFEERED', t2.amount * er.last_currency_rate, 0)) as tail_fee_cny_fi
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_fees_di
                where
                    standard_type_code in ('OMS_AfterSales_STORAGE_BeforeFEERED',
                        'OMS_AfterSales_TAIL_BeforeFEERED')
                        )                             as t1
            join (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_lh_dfc_fc_standard_oms_sale_fees_item_di
                where
                    standard_type_code in ('OMS_AfterSales_STORAGE_BeforeFEERED',
                                           'OMS_AfterSales_TAIL_BeforeFEERED')
                )                             as t2
                on t2.standard_unique_num = t1.standard_unique_num
            join dwd.dwd_dim_exchange_rate_df as er
                on t1.currency_code = er.currency_code and
                   date(t1.order_paid_time) between er.start_date and er.end_date
        where
              t1.rn = 1
          and t2.rn = 1
        group by
            t1.account_id
          , t1.order_no
        )
  , refund_order as (
        select
            account_id
          , order_id
          , sum(refund_amount_orderline) as refund_amount_order
          , max(ref_number)              as ref_number
        from dwd.dwd_fact_tcct_oms_refund_df
        where
              cancel_date is null
          and date(check_time) between '2026-02-01' and '2026-04-30'
          and aftersale_order_status_id = 2
        group by
            account_id
          , order_id
        )
  , fr_order as (
        select distinct
            od.account_id
          , od.order_id
          , ro.ref_number
        from
            refund_order                            as ro
            join dwd.dwd_dim_tcct_oms_orders_fee_di as od
                on od.order_id = ro.order_id and od.account_id = ro.account_id
        where
            ro.refund_amount_order >= od.amount
        )
  , bg_data as (
        select
            account_id
          , order_id
          , sum(stock_fee_cny_fi)           as stock_fee_cny_fi
        , sum(tail_fee_cny_fi) as tail_fee_cny_fi
        from dws.dws_alct_theory_profit_refund_cost_backwash_df
        where
              dt between '2026-02-01' and '2026-04-30'
          and out_qty = 0
        group by
            account_id
          , order_id
        having
            sum(platform_change_fee_cny_fi) + sum(tail_fee_cny_fi)<> 0
        )
  , accounting_order as (
        select *
        from
            (
                select *
                     , row_number() over (partition by id order by dt desc) as rn
                from ods.ods_pbbs_oc_so_order_financial_accounting_list_di
                ) as t
        where
            rn = 1
        )
select
 bg.*, bs.*
from
    bg_data                    as bg
    join fr_order              as fr
        on fr.order_id = bg.order_id and fr.account_id = bg.account_id
    -- right
        join base             as bs
        on bg.account_id = bs.account_id and bg.order_id = bs.order_no
    -- left join accounting_order as ao on bg.order_id = ao.order_no and bg.account_id = ao.account_id
    -- join dwd.dwd_fact_tcct_oms_refund_df as rf on bs.order_no = rf.order_id and bs.account_id = rf.account_id
where
      -- bs.account_id is null and ao.order_no is not null
      -- bg.order_id is null
      abs(bg.stock_fee_cny_fi - bs.stock_fee_cny_fi) >= 1
  or
    abs(bg.tail_fee_cny_fi - bs.tail_fee_cny_fi) >= 1
;
eBay_greens6	19-14171-23383

;
select *
from ods.ods_lh_dfc_fc_standard_oms_sale_fees_di
where
    standard_type_code in ('OMS_AfterSales_STORAGE_BeforeFEERED',
                           'OMS_AfterSales_TAIL_BeforeFEERED')
and order_no = 'PO-210-00174408504953473'
;
select order_source, cancel_type_code
from dwd.dwd_dim_tcct_oms_orders_di
where order_id = '19-14171-23383'
;
select *
from dwd.dwd_fact_tcct_oms_refund_df
where order_id = '19-14171-23383'
;
select *
from ods.ods_lh_dfc_fc_standard_oms_sale_fees_di
where order_no = 'PO-210-00174408504953473'
;