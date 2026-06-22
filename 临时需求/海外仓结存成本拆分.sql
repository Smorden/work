with fnct_batch as (
    select
        batch_code
      , sku
      , fb.warehouse_id
      , unique_no
      , qty
      , amount
      ,calc_time
    from
        dwd.dwd_fact_fnct_batch_balance_di as fb
        join dwd.dwd_dim_warehouse_df      as wh
            on wh.warehouse_id = fb.warehouse_id and wh.warehouse_location_country = 'GB'
    where
          fb.record_status = 1
      and fb.warehouse_location_type = 3
      and fb.qty > 0
    )
,ebay_account as (
    SELECT
        sku ,
        asin ,
        listing_status
    FROM
        ads.ads_sad_product_operation_detail_df
    where account_short_name = 'ebay-UK356'
        and listing_status = 'Active'
    )
,sku_cost as (
    select
        bn.sku
      , bn.unique_no
      , max(bn.qty)                                                                                                  as qty
      , SUM(bnf.settlement_unit_price / er.profit_currency_rate)                                                     AS product_cost
      , SUM(IF(bnf.income_expense_item_2_name = '采购成本', bnf.settlement_unit_price,
               0))                                                                                                   AS purchase_cost_cny
      , SUM(IF(bnf.income_expense_item_2_name = '采购成本', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS purchase_cost
      , SUM(IF(bnf.income_expense_item_2_name = '头程税费', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS top_vat_fee
      , SUM(IF(bnf.income_expense_item_2_name = '头程运费', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS top_freight_fee
      , SUM(IF(bnf.income_expense_item_2_name = '头程杂费', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS top_incidental_fee
      , SUM(IF(bnf.income_expense_item_2_name = '特殊费用', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS special_fee
      , SUM(IF(bnf.income_expense_item_2_name = '成本调整', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS cost_adjustment
      , SUM(IF(bnf.income_expense_item_2_name = '公司间加价', bnf.settlement_unit_price / er.profit_currency_rate,
               0))                                                                                                   AS intercompany_markup_fee
    from
        fnct_batch                                          as bn
        join (
            select distinct sku
            from ebay_account
            )                                               as ea
            on ea.sku = bn.sku
        join dwd.dwd_fact_fnct_cost_order_line_batch_fee_di as bnf
            on bn.unique_no = bnf.unique_no and bnf.record_status = 1
        INNER JOIN dwd.dwd_dim_exchange_rate_df             as er
            ON er.start_date = date_trunc(curdate(), 'month') AND er.currency_code = 'GBP'
    group by
        bn.sku
      , bn.unique_no
    )
, sales_30d as (
    select sku, sum(sales_qty) as sales_qty_30d
    from dws.dws_alct_theory_profit_sum_order_di
    where account_id in ( select account_id
                          from dwd.dwd_dim_oms_account_df
                          where account_short_name = 'ebay-UK356' )
    and site_id = 2
    and dt between date_sub(curdate(), interval 30 day)
        and date_sub(curdate(), interval 1 day)
    group by sku
    )
, dim_sku as (
    select sku, sku_name, sku_en_name
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
    )
select
    ac.sku
     , sku.sku_name as 中文名称
     , sku.sku_en_name as 英文名称
     ,ea.asin as listing
     , ac.purchase_cost_cny_avg as `采购价（rmb）`
    , ac.purchase_cost_avg as `采购价（英镑）`
    , ac.top_freight_fee_avg as `头程运费（英镑）`
    , ac.top_vat_fee_avg as `头程税费（英镑）`
    , ac.top_incidental_fee_avg as `头程杂费（英镑）`
    , ac.special_fee_avg as `头程特殊费用（英镑）`
    , ac.cost_adjustment_avg as `成本调整（英镑）`
    , ac.intercompany_markup_fee_avg as `公司间调整（英镑）`
     , ac.product_cost_avg as `目的仓成本（英镑）`
,ifnull(sa.sales_qty_30d,0) as 最近30天销量
from(
    select sku
        , sum(product_cost * qty) / sum(qty) as product_cost_avg
        , sum(purchase_cost_cny * qty) / sum(qty) as purchase_cost_cny_avg
        , sum(purchase_cost * qty) / sum(qty) as purchase_cost_avg
        , sum(top_freight_fee * qty) / sum(qty) as top_freight_fee_avg
        , sum(top_vat_fee * qty) / sum(qty)  as top_vat_fee_avg
        , sum(top_incidental_fee * qty) / sum(qty) as top_incidental_fee_avg
        , sum(special_fee * qty) / sum(qty) as special_fee_avg
        , sum(cost_adjustment * qty) / sum(qty) as cost_adjustment_avg
        , sum(intercompany_markup_fee * qty) / sum(qty) as intercompany_markup_fee_avg
    from sku_cost
    group by sku
        ) as ac
join dim_sku as sku on sku.sku = ac.sku
left join sales_30d as sa on sa.sku = ac.sku
join ebay_account as ea on ea.sku = ac.sku
order by ac.sku
;