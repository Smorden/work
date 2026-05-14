truncate table  ads.ads_alct_theory_profit_details_di partition(

);
-- begin_insert --
INSERT INTO ads.ads_alct_theory_profit_details_di
(
	dt , sku , platform_id , site_id , platform_name , site_name , operation_mode , oparation_mode , channel_user_id ,
	channel_user_name , rank_score , sab_level , sale_status , stop_purchase_reason , last_stop_purchase_date ,
	forecast_sales , listing_sale_price_range , listing_avg_sale_price , max_sale_price ,
	min_sale_price , listing_currency_code , forecast_sales_amt_cny_fi , sales_amt_cny_fi , sales_days_td ,
	sales_amt_cny_fi_td , forecast_sales_amt_cny_fi_td , avg_sales_amt_cny_fi , sales_target_amt ,
	sales_target_amt_weekly , sales_target_amt_finally , total_cost_cny_fi , product_cost_cny_fi ,
	purchase_cost_cny_fi , top_vat_fee_cny_fi , top_freight_fee_cny_fi , top_incidental_fee_cny_fi ,
	platform_fee_amt_cny_fi , platform_change_fee_cny_fi , platform_fixed_fee_cny_fi , platform_promotion_fee_cny_fi ,
	international_fee_cny_fi , currency_conversion_charge_fee_cny_fi , coupon_fee_cny_fi , paypal_fee_cny_fi ,
	tail_fee_cny_fi , freight_fee_cny_fi , management_fee_cny_fi , remote_fee_amt_cny_fi ,
	overseaswarehouse_handing_fee_cny_fi , out_packing_fee_cny_fi , complex_package_fee_cny_fi ,
	fuel_surcharge_fee_cny_fi , over_length_fee_cny_fi , over_weight_fee_cny_fi , over_volume_fee_cny_fi ,
	busy_season_fee_cny_fi , stock_fee_cny_fi , damage_fee_cny_fi , inventory_loss_fee_cny_fi , aftersale_fee_cny_fi ,
	refund_loss_fee_cny_fi , overseaswarehouse_stock_interest_fee_cny_fi , midwarehouse_stock_interest_fee_cny_fi ,
	stock_interest_fee_cny_fi , sales_gross_profit_cny_fi , ld_fee_cny_fi , bd_fee_cny_fi , vine_fee_cny_fi ,
	in_plat_adv_fee_cny_fi , out_plat_adv_fee_cny_fi , adv_fee_cny_fi , sales_net_profit_cny_fi , sales_qty ,
	sum_sales_qty_7d , sum_sales_qty_14d , sku_category , sku_name , charge_user_id , charge_user_name , is_seasonal ,
	key_word , spu , amazon_ad_order_cnt , amazon_ad_order_sales_amt , ebay_ad_order_sales_amt_cny_fi ,
	aftersale_orders_cnt , bad_goods_out_cost_cny_fi_td , out_cost_cny_fi_td , last_bad_goods_out_cost_cny_fi_td ,
	last_out_cost_cny_fi_td , bad_goods_out_cost_cny_fi_30d , out_cost_cny_fi_30d , last_bad_goods_out_cost_cny_fi_30d ,
	last_out_cost_cny_fi_30d , amazon_impressions , amazon_clicks , ebay_ad_order_cnt , order_cnt_30d ,
	aftersale_amt_cny_fi_30d , paid_sales_amt_cny_fi_30d , stock_turnover_day , received_qty_30d , unqualified_qty_30d ,
	last_received_qty_30d , last_unqualified_qty_30d , spu_cn_name , er_profit_or_loss_amt_cny_fi ,
	current_supply_days , stock_status , unsalable_slowsales_fine , performance_net_profit_cny_fi ,
	actual_inventory_loss_fee_cny_fi , goods_code , sku_version_no , promotion_fee_cny_fi ,
	ebay_supervision_fee_cny_fi , violation_fee_cny_fi , comprehensive_cost_cny_fi , account_period_profit ,
	oversea_stock_fee , sale_status_code , stock_status_select_key , avg_sales_qty_7d , avg_sales_qty_8_14d ,
	sales_change_trend_7d , available_qty , onway_stock_qty ,
	sys_account_id , newest_spu ,
	newest_channel_user_id , newest_channel_user_name , newest_charge_user_id , newest_charge_user_name ,
	newest_is_share_product , newest_department_id , newest_department_1_id , newest_department_1_name ,
	newest_department_2_id , newest_department_2_name , newest_department_3_id , newest_department_3_name ,
	newest_department_4_id , newest_department_4_name , newest_department_5_id , newest_department_5_name ,
	newest_department_6_id , newest_department_6_name , newest_department_7_id , newest_department_7_name ,
	newest_department_8_id , newest_department_8_name , newest_department_9_id , newest_department_9_name ,
	newest_department_10_id , newest_department_10_name , newest_spu_cn_name , newest_sku_name , newest_platform_name ,
	newest_site_name , newest_sale_status , newest_sale_status_code , newest_sale_model , newest_stock_status ,
	newest_stock_status_select_key , newest_tag_code , sku_channel_user_name , spu_channel_user_name ,
	spu_charge_user_name , newest_sku_category , sku_tag , expected_norm_sales_amt , expected_max_sales_amt ,
	min_expect_dt , oversea_stock_qty , expected_norm_sales , expected_norm_sales_price ,
	current_expected_norm_sales_amt , adjustment_fee_cny_fi , depart_adjustment_fee_cny_fi ,
	theoretical_purchase_price , expected_norm_cost , first_inbound_date)
WITH au AS (
           SELECT a.sku
                , a.platform_id
                , a.site_id
                , group_concat(CAST(a.sys_account_id AS VARCHAR(100)), ',') AS sys_account_id
                , MAX(a.spu)                                                AS newest_spu
                , MAX(a.channel_user_id)                                    AS newest_channel_user_id
                , MAX(a.channel_user_name)                                  AS newest_channel_user_name
                , MAX(a.charge_user_id)                                     AS newest_charge_user_id
                , MAX(a.charge_user_name)                                   AS newest_charge_user_name
                , MAX(a.is_share_product)                                   AS newest_is_share_product
                , MAX(a.department_id)                                      AS newest_department_id
                , MAX(a.department_1_id)                                    AS newest_department_1_id
                , MAX(a.department_1_name)                                  AS newest_department_1_name
                , MAX(a.department_2_id)                                    AS newest_department_2_id
                , MAX(a.department_2_name)                                  AS newest_department_2_name
                , MAX(a.department_3_id)                                    AS newest_department_3_id
                , MAX(a.department_3_name)                                  AS newest_department_3_name
                , MAX(a.department_4_id)                                    AS newest_department_4_id
                , MAX(a.department_4_name)                                  AS newest_department_4_name
                , MAX(a.department_5_id)                                    AS newest_department_5_id
                , MAX(a.department_5_name)                                  AS newest_department_5_name
                , MAX(a.department_6_id)                                    AS newest_department_6_id
                , MAX(a.department_6_name)                                  AS newest_department_6_name
                , MAX(a.department_7_id)                                    AS newest_department_7_id
                , MAX(a.department_7_name)                                  AS newest_department_7_name
                , MAX(a.department_8_id)                                    AS newest_department_8_id
                , MAX(a.department_8_name)                                  AS newest_department_8_name
                , MAX(a.department_9_id)                                    AS newest_department_9_id
                , MAX(a.department_9_name)                                  AS newest_department_9_name
                , MAX(a.department_10_id)                                   AS newest_department_10_id
                , MAX(a.department_10_name)                                 AS newest_department_10_name
                , MAX(a.spu_cn_name)                                        AS newest_spu_cn_name
                , MAX(a.sku_name)                                           AS newest_sku_name
                , MAX(a.platform_name)                                      AS newest_platform_name
                , MAX(a.site_name)                                          AS newest_site_name
                , MAX(a.sale_status)                                        AS newest_sale_status
                , MAX(a.sale_status_code)                                   AS newest_sale_status_code
                , MAX(a.sale_model)                                         AS newest_sale_model
                , MAX(a.stock_status)                                       AS newest_stock_status
                , MAX(a.stock_status_select_key)                            AS newest_stock_status_select_key
                , group_concat(DISTINCT b.tag_code, ',')                    AS newest_tag_code
                , MAX(sku_category)                                         AS sku_category
                , MAX(sku_tag)                                              AS sku_tag
                , MAX(sku_status)                                           AS sku_status
                , MAX(sku_channel_user_name)                                AS sku_channel_user_name
                , MAX(spu_channel_user_name)                                AS spu_channel_user_name
                , MAX(spu_charge_user_name)                                 AS spu_charge_user_name
, MIN(first_inbound_date)                                   AS first_inbound_date
           FROM dwd.dwd_dim_sku_site_plat_user_dept_rel_df a
           LEFT JOIN dwd.dwd_dim_sku_tag_df b
               ON a.sku = b.sku
           GROUP BY sku
                  , platform_id
                  , site_id
           )
SELECT a.dt                                                                     AS dt
     , a.sku                                                                    AS sku
     , a.platform_id                                                            AS platform_id
     , a.site_id                                                                AS site_id
     , a.platform_name                                                          AS platform_name
     , a.site_name                                                              AS site_name
     , a.sale_model                                                             AS operation_mode
     , a.sale_model                                                             AS oparation_mode
     , a.channel_user_id                                                        AS channel_user_id
     , a.channel_user_name                                                      AS channel_user_name
     , a.listing_rank_score                                                     AS rank_score
     , a.sab_level                                                              AS sab_level
     , a.sale_status                                                            AS sale_status
     , a.stop_purchase_reason                                                   AS stop_purchase_reason
     , a.last_stop_purchase_date                                                AS last_stop_purchase_date
     , a.forecast_sales                                                         AS forecast_sales
     , a.listing_sale_price_range                                               AS listing_sale_price_range
     , a.listing_avg_sale_price                                                 AS listing_avg_sale_price
     , a.listing_max_sale_price                                                 AS max_sale_price
     , a.listing_min_sale_price                                                 AS min_sale_price
     , a.listing_currency_code                                                  AS listing_currency_code
     , a.forecast_sales_amt_cny_fi                                              AS forecast_sales_amt_cny_fi
     , a.sales_amt_cny_fi                                                       AS sales_amt_cny_fi
     , a.sales_days_td                                                          AS sales_days_td
     , a.sales_amt_cny_fi_td                                                    AS sales_amt_cny_fi_td
     , a.forecast_sales_amt_cny_fi_td                                           AS forecast_sales_amt_cny_fi_td
     , a.avg_sales_amt_cny_fi                                                   AS avg_sales_amt_cny_fi
     , a.sales_target_amt                                                       AS sales_target_amt
     , a.sales_target_amt_weekly                                                AS sales_target_amt_weekly
     , a.sales_target_amt_finally                                               AS sales_target_amt_finally
     , -1 * a.total_cost_cny_fi                                                 AS total_cost_cny_fi
     , -1 * a.product_cost_cny_fi                                               AS product_cost_cny_fi
     , -1 * a.purchase_cost_cny_fi                                              AS purchase_cost_cny_fi
     , -1 * a.top_vat_fee_cny_fi                                                AS top_vat_fee_cny_fi
     , -1 * a.top_freight_fee_cny_fi                                            AS top_freight_fee_cny_fi
     , -1 * a.top_incidental_fee_cny_fi                                         AS top_incidental_fee_cny_fi
     , -1 * a.platform_fee_amt_cny_fi                                           AS platform_fee_amt_cny_fi
     , -1 * a.platform_change_fee_cny_fi                                        AS platform_change_fee_cny_fi
     , -1 * a.platform_fixed_fee_cny_fi                                         AS platform_fixed_fee_cny_fi
     , -1 * a.platform_promotion_fee_cny_fi                                     AS platform_promotion_fee_cny_fi
     , -1 * a.international_fee_cny_fi                                          AS international_fee_cny_fi
     , -1 * a.currency_conversion_charge_fee_cny_fi                             AS currency_conversion_charge_fee_cny_fi
     , -1 * a.coupon_fee_cny_fi                                                 AS coupon_fee_cny_fi
     , -1 * a.paypal_fee_cny_fi                                                 AS paypal_fee_cny_fi
     , -1 * a.tail_fee_cny_fi                                                   AS tail_fee_cny_fi
     , -1 * a.freight_fee_cny_fi                                                AS freight_fee_cny_fi
     , -1 * a.management_fee_cny_fi                                             AS management_fee_cny_fi
     , -1 * a.remote_fee_amt_cny_fi                                             AS remote_fee_amt_cny_fi
     , -1 * a.overseaswarehouse_handing_fee_cny_fi                              AS overseaswarehouse_handing_fee_cny_fi
     , -1 * a.out_packing_fee_cny_fi                                            AS out_packing_fee_cny_fi
     , -1 * a.complex_package_fee_cny_fi                                        AS complex_package_fee_cny_fi
     , -1 * a.fuel_surcharge_fee_cny_fi                                         AS fuel_surcharge_fee_cny_fi
     , -1 * a.over_length_fee_cny_fi                                            AS over_length_fee_cny_fi
     , -1 * a.over_weight_fee_cny_fi                                            AS over_weight_fee_cny_fi
     , -1 * a.over_volume_fee_cny_fi                                            AS over_volume_fee_cny_fi
     , -1 * a.busy_season_fee_cny_fi                                            AS busy_season_fee_cny_fi
     , -1 * a.stock_fee_cny_fi                                                  AS stock_fee_cny_fi
     , -1 * a.damage_fee_cny_fi                                                 AS damage_fee_cny_fi
     , -1 * a.inventory_loss_fee_cny_fi                                         AS inventory_loss_fee_cny_fi
     , -1 * a.aftersale_fee_cny_fi                                              AS aftersale_fee_cny_fi
     , -1 * a.refund_loss_fee_cny_fi                                            AS refund_loss_fee_cny_fi
     , -1 * a.overseaswarehouse_stock_interest_fee_cny_fi                       AS overseaswarehouse_stock_interest_fee_cny_fi
     , -1 * a.midwarehouse_stock_interest_fee_cny_fi                            AS midwarehouse_stock_interest_fee_cny_fi
     , -1 * a.stock_interest_fee_cny_fi                                         AS stock_interest_fee_cny_fi
     , a.sales_gross_profit_cny_fi                                              AS sales_gross_profit_cny_fi
     , -1 * a.ld_fee_cny_fi                                                     AS ld_fee_cny_fi
     , -1 * a.bd_fee_cny_fi                                                     AS bd_fee_cny_fi
     , -1 * a.vine_fee_cny_fi                                                   AS vine_fee_cny_fi
     , -1 * a.in_plat_adv_fee_cny_fi                                            AS in_plat_adv_fee_cny_fi
     , -1 * a.out_plat_adv_fee_cny_fi                                           AS out_plat_adv_fee_cny_fi
     , -1 * a.adv_fee_cny_fi                                                    AS adv_fee_cny_fi
     , a.sales_net_profit_cny_fi                                                AS sales_net_profit_cny_fi
     , a.sales_qty                                                              AS sales_qty
     , a.sum_sales_qty_7d                                                       AS sum_sales_qty_7d
     , a.sum_sales_qty_14d                                                      AS sum_sales_qty_14d
     , a.sku_category                                                           AS sku_category
     , a.sku_name                                                               AS sku_name
     , a.charge_user_id                                                         AS charge_user_id
     , a.charge_user_name                                                       AS charge_user_name
     , a.is_seasonal                                                            AS is_seasonal
     , a.key_word                                                               AS key_word
     , a.spu                                                                    AS spu
     , a.amazon_ad_order_cnt                                                    AS amazon_ad_order_cnt
     , a.amazon_ad_order_sales_amt                                              AS amazon_ad_order_sales_amt
     , a.ebay_ad_order_sales_amt_cny_fi                                         AS ebay_ad_order_sales_amt_cny_fi
     , a.aftersale_orders_cnt                                                   AS aftersale_orders_cnt
     , -1 * a.bad_goods_out_cost_cny_fi_td                                      AS bad_goods_out_cost_cny_fi_td
     , -1 * a.out_cost_cny_fi_td                                                AS out_cost_cny_fi_td
     , -1 * a.last_bad_goods_out_cost_cny_fi_td                                 AS last_bad_goods_out_cost_cny_fi_td
     , -1 * a.last_out_cost_cny_fi_td                                           AS last_out_cost_cny_fi_td
     , -1 * a.bad_goods_out_cost_cny_fi_30d                                     AS bad_goods_out_cost_cny_fi_30d
     , -1 * a.out_cost_cny_fi_30d                                               AS out_cost_cny_fi_30d
     , -1 * a.last_bad_goods_out_cost_cny_fi_30d                                AS last_bad_goods_out_cost_cny_fi_30d
     , -1 * a.last_out_cost_cny_fi_30d                                          AS last_out_cost_cny_fi_30d
     , a.amazon_impressions                                                     AS amazon_impressions
     , a.amazon_clicks                                                          AS amazon_clicks
     , a.ebay_ad_order_cnt                                                      AS ebay_ad_order_cnt
     , a.order_cnt_30d                                                          AS order_cnt_30d
     , -1 * a.aftersale_amt_cny_fi_30d                                          AS aftersale_amt_cny_fi_30d
     , a.paid_sales_amt_cny_fi_30d                                              AS paid_sales_amt_cny_fi_30d
     , a.stock_turnover_day                                                     AS stock_turnover_day
     , a.received_qty_30d                                                       AS received_qty_30d
     , a.unqualified_qty_30d                                                    AS unqualified_qty_30d
     , a.last_received_qty_30d                                                  AS last_received_qty_30d
     , a.last_unqualified_qty_30d                                               AS last_unqualified_qty_30d
     , a.spu_cn_name                                                            AS spu_cn_name
     , -1 * a.er_profit_or_loss_amt_cny_fi                                           AS er_profit_or_loss_amt_cny_fi
     , a.current_supply_days                                                    AS current_supply_days
     , a.stock_status                                                           AS stock_status
     , -1 * a.unsalable_slowsales_fine                                          AS unsalable_slowsales_fine
     , a.performance_net_profit_cny_fi                                          AS performance_net_profit_cny_fi
     , -1 * a.actual_inventory_loss_fee_cny_fi                                  AS actual_inventory_loss_fee_cny_fi
     , a.goods_code                                                             AS goods_code
     , a.sku_version_no                                                         AS sku_version_no
     , -1 * a.promotion_fee_cny_fi                                              AS promotion_fee_cny_fi
     , -1 * a.ebay_supervision_fee_cny_fi                                       AS ebay_supervision_fee_cny_fi
     , -1 * a.violation_fee_cny_fi                                              AS violation_fee_cny_fi
     , 0                                                                        AS comprehensive_cost_cny_fi
     , 0                                                                        AS account_period_profit
     , -1 * a.oversea_stock_fee                                                 AS oversea_stock_fee
     , st.sale_status_code                                                      AS sale_status_code
     , NVL(sk.selection_key, '')                                                AS stock_status_select_key
     , a.sum_sales_qty_7d / 7                                                   AS avg_sales_qty_7d
     , NVL(( a.sum_sales_qty_14d - a.sum_sales_qty_7d ) / 7, 0)                 AS avg_sales_qty_8_14d
     , CASE
           WHEN a.sum_sales_qty_14d - a.sum_sales_qty_7d * 2 > 0 THEN '下降'
           WHEN a.sum_sales_qty_14d - a.sum_sales_qty_7d * 2 = 0 THEN '持平'
           WHEN a.sum_sales_qty_14d - a.sum_sales_qty_7d * 2 < 0 THEN '上升'
           ELSE '' END                                                                 AS sales_change_trend_7d
     , NVL(sh.available_qty, 0)                                                 AS available_qty
     , NVL(sh.onway_stock_qty, 0)                                               AS onway_stock_qty
     , au.sys_account_id                                                        AS sys_account_id
     , au.newest_spu                                                            AS newest_spu
     , au.newest_channel_user_id                                                AS newest_channel_user_id
     , au.newest_channel_user_name                                              AS newest_channel_user_name
     , au.newest_charge_user_id                                                 AS newest_charge_user_id
     , au.newest_charge_user_name                                               AS newest_charge_user_name
     , au.newest_is_share_product                                               AS newest_is_share_product
     , au.newest_department_id                                                  AS newest_department_id
     , au.newest_department_1_id                                                AS newest_department_1_id
     , au.newest_department_1_name                                              AS newest_department_1_name
     , au.newest_department_2_id                                                AS newest_department_2_id
     , au.newest_department_2_name                                              AS newest_department_2_name
     , au.newest_department_3_id                                                AS newest_department_3_id
     , au.newest_department_3_name                                              AS newest_department_3_name
     , au.newest_department_4_id                                                AS newest_department_4_id
     , au.newest_department_4_name                                              AS newest_department_4_name
     , au.newest_department_5_id                                                AS newest_department_5_id
     , au.newest_department_5_name                                              AS newest_department_5_name
     , au.newest_department_6_id                                                AS newest_department_6_id
     , au.newest_department_6_name                                              AS newest_department_6_name
     , au.newest_department_7_id                                                AS newest_department_7_id
     , au.newest_department_7_name                                              AS newest_department_7_name
     , au.newest_department_8_id                                                AS newest_department_8_id
     , au.newest_department_8_name                                              AS newest_department_8_name
     , au.newest_department_9_id                                                AS newest_department_9_id
     , au.newest_department_9_name                                              AS newest_department_9_name
     , au.newest_department_10_id                                               AS newest_department_10_id
     , au.newest_department_10_name                                             AS newest_department_10_name
     , au.newest_spu_cn_name                                                    AS newest_spu_cn_name
     , au.newest_sku_name                                                       AS newest_sku_name
     , au.newest_platform_name                                                  AS newest_platform_name
     , au.newest_site_name                                                      AS newest_site_name
     , au.newest_sale_status                                                    AS newest_sale_status
     , au.newest_sale_status_code                                               AS newest_sale_status_code
     , au.newest_sale_model                                                     AS newest_sale_model
     , au.newest_stock_status                                                   AS newest_stock_status
     , au.newest_stock_status_select_key                                        AS newest_stock_status_select_key
     , au.newest_tag_code                                                       AS newest_tag_code
     , au.sku_channel_user_name                                                 AS sku_channel_user_name
     , au.spu_channel_user_name                                                 AS spu_channel_user_name
     , au.spu_charge_user_name                                                  AS spu_charge_user_name
     , au.sku_category                                                          AS newest_sku_category
     , au.sku_tag                                                               AS sku_tag
     , IF(a.dt >= '2024-07-01', ifnull(ex.expected_norm_sales_amt, 0)
    , a.sales_amt_cny_fi)                                                   AS expected_norm_sales_amt
     , IF(a.dt >= '2024-07-01', ifnull(ex.expected_max_sales_amt, 0)
    , a.sales_amt_cny_fi)                                                   AS expected_max_sales_amt
     , IF(a.dt < md.min_expect_dt, NULL, md.min_expect_dt)                      AS min_expect_dt
     , ifnull(ex.oversea_stock_qty, 0)                                          AS oversea_stock_qty
     , IF(a.dt >= '2024-07-01', ifnull(ex.expected_norm_sales, 0), a.sales_qty) AS expected_norm_sales
     , IF(a.dt >= '2024-07-01', ifnull(ex.expected_sales_price, 0)
    , ifnull(a.sales_amt_cny_fi / a.sales_qty, 0))                          AS expected_norm_sales_price
     , IF(a.dt >= '2024-07-01', ifnull(mx.expected_norm_sales_amt, 0)
    , a.sales_amt_cny_fi)                                                   AS current_expected_norm_sales_amt
     , -1 * a.adjustment_fee_cny_fi                                             AS adjustment_fee_cny_fi
     , 0                                                                        AS depart_adjustment_fee_cny_fi
     , his_sku.theoretical_purchase_price                                       as theoretical_purchase_price
     , his_sku.theoretical_purchase_price
    * IF(a.dt >= '2024-07-01', ifnull(ex.expected_norm_sales, 0), a.sales_qty) as expected_norm_cost
     , au.first_inbound_date as first_inbound_date
FROM dws.dws_opct_sku_plat_site_div_details_di      a
INNER JOIN au
    ON a.sku = au.sku
    AND a.platform_id = au.platform_id
    AND a.site_id = au.site_id
LEFT JOIN dwd.dwd_dim_sale_status_df                st
    ON IF(a.sale_status = '', '其他', a.sale_status) = st.sale_status_name
    AND st.used_for_system = 'SYS002'
LEFT JOIN dwd.dwd_dim_dashboard_filter_selection_nf sk
    ON NVL(a.stock_status, '正常') = sk.selection_value
    AND sk.remark = '库存健康类型'
LEFT JOIN (
          SELECT dt, sku, theoretical_purchase_price
          FROM dwd.dwd_dim_sku_ds
          WHERE dt BETWEEN @start_dt AND DATE_SUB(curdate(), INTERVAL 1 DAY)
          )                                     his_sku
    ON a.dt = his_sku.dt and a.sku = his_sku.sku
LEFT JOIN (
          SELECT dt
               , sku
               , platform_id
               , site_id
               , expected_norm_sales
               , expected_sales_price
               , expected_norm_sales * expected_sales_price AS expected_norm_sales_amt
               , oversea_stock_qty
               , enable_status
               , expected_max_sales * expected_sales_price  AS expected_max_sales_amt
          FROM dws.dws_alct_expected_his_sales_channel_ds
          WHERE dt BETWEEN @start_dt AND DATE_SUB(curdate(), INTERVAL 1 DAY)
          ) AS                                  ex
    ON ex.dt = a.dt
    AND ex.sku = a.sku
    AND ex.platform_id = a.platform_id
    AND ex.site_id = a.site_id
JOIN (
     SELECT ifnull(MIN(dt), '9999-12-31') AS min_expect_dt
     FROM dws.dws_alct_expected_future_sales_channel_his_fix_ds
     )      AS                                  md
LEFT JOIN (
          SELECT dt, sku, platform_id, site_id, expected_norm_sales_amt
          FROM dws.dws_alct_expected_future_sales_channel_his_fix_ds
          WHERE dt BETWEEN @start_dt AND DATE_SUB(curdate(), INTERVAL 1 DAY)
          ) AS                                  mx
    ON mx.dt = a.dt
    AND mx.sku = a.sku
    AND mx.platform_id = a.platform_id
    AND mx.site_id = a.site_id
LEFT JOIN (
          SELECT dt
               , sku
               , site_id
               , sale_model
               , SUM(available_qty)   AS available_qty
               , SUM(onway_stock_qty) AS onway_stock_qty
          FROM dws.dws_ivct_stock_health_dep_site_di
          WHERE dt BETWEEN @start_dt AND DATE_SUB(curdate(), INTERVAL 1 DAY)
          GROUP BY dt
                 , sku
                 , site_id
                 , sale_model
          )                                     sh
    ON a.dt = sh.dt
    AND a.sku = sh.sku
    AND a.site_id = sh.site_id
    AND ifnull(a.sale_model, 'FBM') = sh.sale_model
WHERE a.dt BETWEEN@start_dt AND DATE_SUB(curdate(), INTERVAL 1 DAY)
  AND ( ifnull(au.sku_status, '') <> '结束'
	OR a.sales_amt_cny_fi <> 0
	OR a.sales_net_profit_cny_fi <> 0
	OR a.performance_net_profit_cny_fi <> 0
	OR a.amazon_impressions <> 0
	OR a.amazon_ad_order_cnt <> 0
	OR a.adv_fee_cny_fi <> 0
	OR a.paid_sales_amt_cny_fi_30d <> 0
	OR a.aftersale_amt_cny_fi_30d <> 0
	OR ifnull(ex.expected_norm_sales_amt, 0) <> 0
	)

UNION ALL

SELECT dt                           AS dt
	 , ''                           AS sku
	 , NULL                         AS platform_id
	 , NULL                         AS site_id
	 , NULL                         AS platform_name
	 , NULL                         AS site_name
	 , NULL                         AS operation_mode
	 , NULL                         AS oparation_mode
	 , NULL                         AS channel_user_id
	 , NULL                         AS channel_user_name
	 , NULL                         AS rank_score
	 , NULL                         AS sab_level
	 , NULL                         AS sale_status
	 , NULL                         AS stop_purchase_reason
	 , NULL                         AS last_stop_purchase_date
	 , NULL                         AS forecast_sales
	 , NULL                         AS listing_sale_price_range
	 , NULL                         AS listing_avg_sale_price
	 , NULL                         AS max_sale_price
	 , NULL                         AS min_sale_price
	 , NULL                         AS listing_currency_code
	 , NULL                         AS forecast_sales_amt_cny_fi
	 , NULL                         AS sales_amt_cny_fi
	 , NULL                         AS sales_days_td
	 , NULL                         AS sales_amt_cny_fi_td
	 , NULL                         AS forecast_sales_amt_cny_fi_td
	 , NULL                         AS avg_sales_amt_cny_fi
	 , NULL                         AS sales_target_amt
	 , NULL                         AS sales_target_amt_weekly
	 , NULL                         AS sales_target_amt_finally
	 , SUM(adjustment_fee_cny)      AS total_cost_cny_fi
	 , NULL                         AS product_cost_cny_fi
	 , NULL                         AS purchase_cost_cny_fi
	 , NULL                         AS top_vat_fee_cny_fi
	 , NULL                         AS top_freight_fee_cny_fi
	 , NULL                         AS top_incidental_fee_cny_fi
	 , NULL                         AS platform_fee_amt_cny_fi
	 , NULL                         AS platform_change_fee_cny_fi
	 , NULL                         AS platform_fixed_fee_cny_fi
	 , NULL                         AS platform_promotion_fee_cny_fi
	 , NULL                         AS international_fee_cny_fi
	 , NULL                         AS currency_conversion_charge_fee_cny_fi
	 , NULL                         AS coupon_fee_cny_fi
	 , NULL                         AS paypal_fee_cny_fi
	 , NULL                         AS tail_fee_cny_fi
	 , NULL                         AS freight_fee_cny_fi
	 , NULL                         AS management_fee_cny_fi
	 , NULL                         AS remote_fee_amt_cny_fi
	 , NULL                         AS overseaswarehouse_handing_fee_cny_fi
	 , NULL                         AS out_packing_fee_cny_fi
	 , NULL                         AS complex_package_fee_cny_fi
	 , NULL                         AS fuel_surcharge_fee_cny_fi
	 , NULL                         AS over_length_fee_cny_fi
	 , NULL                         AS over_weight_fee_cny_fi
	 , NULL                         AS over_volume_fee_cny_fi
	 , NULL                         AS busy_season_fee_cny_fi
	 , NULL                         AS stock_fee_cny_fi
	 , NULL                         AS damage_fee_cny_fi
	 , NULL                         AS inventory_loss_fee_cny_fi
	 , NULL                         AS aftersale_fee_cny_fi
	 , NULL                         AS refund_loss_fee_cny_fi
	 , NULL                         AS overseaswarehouse_stock_interest_fee_cny
	 , NULL                         AS midwarehouse_stock_interest_fee_cny_fi
	 , NULL                         AS stock_interest_fee_cny_fi
	 , NULL                         AS sales_gross_profit_cny_fi
	 , NULL                         AS ld_fee_cny_fi
	 , NULL                         AS bd_fee_cny_fi
	 , NULL                         AS vine_fee_cny_fi
	 , NULL                         AS in_plat_adv_fee_cny_fi
	 , NULL                         AS out_plat_adv_fee_cny_fi
	 , NULL                         AS adv_fee_cny_fi
	 , SUM(adjustment_fee_cny) * -1 AS sales_net_profit_cny_fi
	 , NULL                         AS sales_qty
	 , NULL                         AS sum_sales_qty_7d
	 , NULL                         AS sum_sales_qty_14d
	 , NULL                         AS sku_category
	 , NULL                         AS sku_name
	 , NULL                         AS charge_user_id
	 , NULL                         AS charge_user_name
	 , NULL                         AS is_seasonal
	 , NULL                         AS key_word
	 , NULL                         AS spu
	 , NULL                         AS amazon_ad_order_cnt
	 , NULL                         AS amazon_ad_order_sales_amt
	 , NULL                         AS ebay_ad_order_sales_amt_cny_fi
	 , NULL                         AS aftersale_orders_cnt
	 , NULL                         AS bad_goods_out_cost_cny_fi_td
	 , NULL                         AS out_cost_cny_fi_td
	 , NULL                         AS last_bad_goods_out_cost_cny_fi_td
	 , NULL                         AS last_out_cost_cny_fi_td
	 , NULL                         AS bad_goods_out_cost_cny_fi_30d
	 , NULL                         AS out_cost_cny_fi_30d
	 , NULL                         AS last_bad_goods_out_cost_cny_fi_30d
	 , NULL                         AS last_out_cost_cny_fi_30d
	 , NULL                         AS amazon_impressions
	 , NULL                         AS amazon_clicks
	 , NULL                         AS ebay_ad_order_cnt
	 , NULL                         AS order_cnt_30d
	 , NULL                         AS aftersale_amt_cny_fi_30d
	 , NULL                         AS paid_sales_amt_cny_fi_30d
	 , NULL                         AS stock_turnover_day
	 , NULL                         AS received_qty_30d
	 , NULL                         AS unqualified_qty_30d
	 , NULL                         AS last_received_qty_30d
	 , NULL                         AS last_unqualified_qty_30d
	 , NULL                         AS spu_cn_name
	 , NULL                         AS er_profit_or_loss_amt_cny_fi
	 , NULL                         AS current_supply_days
	 , NULL                         AS stock_status
	 , NULL                         AS unsalable_slowsales_fine
	 , SUM(adjustment_fee_cny) * -1 AS performance_net_profit_cny_fi
	 , NULL                         AS actual_inventory_loss_fee_cny_fi
	 , NULL                         AS goods_code
	 , NULL                         AS sku_version_no
	 , NULL                         AS promotion_fee_cny_fi
	 , NULL                         AS ebay_supervision_fee_cny_fi
	 , NULL                         AS violation_fee_cny_fi
	 , NULL                         AS comprehensive_cost_cny_fi
	 , NULL                         AS account_period_profit
	 , NULL                         AS oversea_stock_fee
	 , NULL                         AS sale_status_code
	 , NULL                         AS stock_status_select_key
	 , NULL                         AS avg_sales_qty_7d
	 , NULL                         AS avg_sales_qty_8_14d
	 , NULL                         AS sales_change_trend_7d
	 , NULL                         AS available_qty
	 , NULL                         AS onway_stock_qty
	 , NULL                         AS sys_account_id
	 , NULL                         AS newest_spu
	 , NULL                         AS newest_channel_user_id
	 , NULL                         AS newest_channel_user_name
	 , NULL                         AS newest_charge_user_id
	 , NULL                         AS newest_charge_user_name
	 , NULL                         AS newest_is_share_product
	 , NULL                         AS newest_department_id
	 , NULL                         AS newest_department_1_id
	 , NULL                         AS newest_department_1_name
	 , NULL                         AS newest_department_2_id
	 , NULL                         AS newest_department_2_name
	 , department_3_id              AS newest_department_3_id
	 , department_3_name            AS newest_department_3_name
	 , department_4_id              AS newest_department_4_id
	 , department_4_name            AS newest_department_4_name
	 , department_5_id              AS newest_department_5_id
	 , department_5_name            AS newest_department_5_name
	 , NULL                         AS newest_department_6_id
	 , NULL                         AS newest_department_6_name
	 , NULL                         AS newest_department_7_id
	 , NULL                         AS newest_department_7_name
	 , NULL                         AS newest_department_8_id
	 , NULL                         AS newest_department_8_name
	 , NULL                         AS newest_department_9_id
	 , NULL                         AS newest_department_9_name
	 , NULL                         AS newest_department_10_id
	 , NULL                         AS newest_department_10_name
	 , NULL                         AS newest_spu_cn_name
	 , NULL                         AS newest_sku_name
	 , NULL                         AS newest_platform_name
	 , NULL                         AS newest_site_name
	 , NULL                         AS newest_sale_status
	 , NULL                         AS newest_sale_status_code
	 , NULL                         AS newest_sale_model
	 , NULL                         AS newest_stock_status
	 , NULL                         AS newest_stock_status_select_key
	 , NULL                         AS newest_tag_code
	 , NULL                         AS sku_channel_user_name
	 , NULL                         AS spu_channel_user_name
	 , NULL                         AS spu_charge_user_name
	 , NULL                         AS newest_sku_category
	 , NULL                         AS sku_tag
	 , NULL                         AS expected_norm_sales_amt
	 , NULL                         AS expected_max_sales_amt
	 , NULL                         AS min_expect_dt
	 , NULL                         AS oversea_stock_qty
	 , NULL                         AS expected_norm_sales
	 , NULL                         AS expected_norm_sales_price
	 , NULL                         AS current_expected_norm_sales_amt
	 , SUM(adjustment_fee_cny)      AS adjustment_fee_cny_fi
	 , SUM(adjustment_fee_cny)      AS depart_adjustment_fee_cny_fi
     , NULL                         AS theoretical_purchase_price
     , NULL                         AS expected_norm_cost
     , NULL AS first_inbound_date
FROM dws.dws_alct_fi_fee_share_import_department_di
where dt between@start_dt and date_sub(curdate(), interval 1 day)
GROUP BY dt
	   , department_3_id
	   , department_3_name
	   , department_4_id
	   , department_4_name
	   , department_5_id
	   , department_5_name
;