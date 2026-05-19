SELECT
    t1.dt as 日期,
    t1.account_short_name as 账户,
    if(t1.is_local=1,'本土','非本土') as 账户类型,
    t1.first_inbound_date as 首次上架日期,
    t1.sku_category AS 品类,
    t1.spu AS SPU编码,
    t1.sku AS SKU编码,
    t1.site_name AS 站点,
    t1.oparation_mode AS 运营模式,
    t1.platform_name AS 平台,
    CONCAT_WS('/', t1.department_3_name, t1.department_4_name, t1.department_5_name) AS 部门,
    t1.charge_user_name AS 开发负责人,
    t1.channel_user_name AS 运营负责人,
    IF(t1.is_seasonal = 1, '是', '否') AS 是否是季节性产品,
    t1.rank_score AS 产品等级,

    -- 销售核心指标
    t1.sales_amt_cny_fi AS 销售收入_不含税,
    t1.sales_qty AS 销售量,
    t1.avg_sales_qty_7d AS `7天日均销量`,
    t1.max_sales_qty AS 最高销量,

    -- 销售毛利率（含分子分母）
    t1.sales_gross_profit_cny_fi AS 销售毛利,
    -- t1.sales_gross_profit_cny_fi AS 销售毛利率,

    -- 销售净利率（含分子分母）
    t1.sales_net_profit_cny_fi AS 销售净利额,
    -- IFNULL(ROUND(t1.sales_net_profit_rate * 100, 2), 0) AS 销售净利率_百分比,

    -- 广告核心指标（从子查询原始字段计算，注释标注计算逻辑）
    IFNULL(t1.amazon_impressions, 0) AS 曝光量,
    IFNULL(t1.amazon_clicks, 0) AS 点击量,
    -- CTR：点击量 / 曝光量 * 100
    -- IFNULL(ROUND(t1.amazon_clicks / NULLIF(t1.amazon_impressions, 0) * 100, 2), 0) AS CTR,
    IFNULL(t1.ad_order_qty, 0) AS 广告订单,
    -- CVR：广告订单 / 点击量 * 100
    -- IFNULL(ROUND(t1.ad_order_qty / NULLIF(t1.amazon_clicks, 0) * 100, 2), 0) AS CVR,
    t1.adv_fee_cny_fi AS 广告花费,
    t1.ad_sales_amt AS 广告销售额,
    -- ACOS：广告花费 / 广告销售额 * 100
    -- IFNULL(ROUND(t1.adv_fee_cny_fi / NULLIF(t1.ad_sales_amt, 0) * 100, 2), 0) AS ACOS,
    -- ACoAS：广告花费 / 销售收入 * 100
    -- IFNULL(ROUND(t1.adv_fee_cny_fi / NULLIF(t1.sales_amt_cny_fi, 0) * 100, 2), 0) AS ACoAS,
    -- 广告订单比例：广告订单 / 总订单数 * 100
    -- IFNULL(ROUND(t1.ad_order_qty / NULLIF(t1.order_cnt_30d, 0) * 100, 2), 0) AS 广告订单比例,
    -- 库存相关指标
    t2.stock_status AS 最近一天的当前库存状态,
    t2.available_qty AS 可用库存,
    t2.forecast_sales AS 预计销量,
    t2.onway_stock_qty AS 在途库存数量,
    CEIL(t2.stock_turnover_day) AS 库存周转天数,
    -- 30天质检不合格率（含分子分母）
    -- t2.unqualified_qty_30d AS 30天质检不合格率_分子,
    -- t2.received_qty_30d AS 30天质检不合格率_分母,
    ROUND(t2.unqualified_rate_30d * 100, 2) AS `30天质检不合格率_百分比`,

    -- 30天海外仓坏品率（含分子分母）
    -- t2.bad_goods_out_cost_cny_fi_30d AS 30天海外仓坏品率_分子,
    -- t2.out_cost_cny_fi_30d AS 30天海外仓坏品率_分母,
    ROUND(t2.bad_goods_rate_overseaswarehouse_30d * 100, 2) AS `30天海外仓坏品率_百分比`,

    -- 累计海外仓坏品率（含分子分母）
    -- t2.bad_goods_out_cost_cny_fi_td AS 累计海外仓坏品率_分子,
    -- t2.out_cost_cny_fi_td AS 累计海外仓坏品率_分母,
    ROUND(t2.bad_goods_rate_overseaswarehouse_td * 100, 2) AS 累计海外仓坏品率_百分比
FROM (
         SELECT dt,account_short_name, is_local,
                sku,
                site_id,
                platform_id,
                sku_category AS sku_category,
                spu AS spu,
                newest_spu_cn_name AS spu_name,
                newest_sku_name AS sku_name,
                key_word AS key_word,
                site_name AS site_name,
                oparation_mode AS oparation_mode,
                platform_name AS platform_name,
                is_seasonal AS is_seasonal,
                rank_score AS rank_score,
                -- 销售原始聚合字段
                sales_amt_cny_fi AS sales_amt_cny_fi,
                sales_qty AS sales_qty,
                sales_qty AS max_sales_qty,
                sales_gross_profit_cny_fi AS sales_gross_profit_cny_fi,
                sales_net_profit_cny_fi AS sales_net_profit_cny_fi,
                avg_sales_qty_7d AS avg_sales_qty_7d,
                -- 组织架构原始字段
                newest_department_3_id AS department_3_id,
                newest_department_3_name AS department_3_name,
                newest_department_4_id AS department_4_id,
                newest_department_4_name AS department_4_name,
                newest_department_5_id AS department_5_id,
                newest_department_5_name AS department_5_name,
                -- 负责人原始字段
                newest_charge_user_id AS charge_user_id,
                newest_charge_user_name AS charge_user_name,
                newest_channel_user_id AS channel_user_id,
                newest_channel_user_name AS channel_user_name,
                -- 广告原始聚合字段（仅保留基础数值，比率在外层计算）
                amazon_impressions  AS amazon_impressions,
                amazon_clicks  AS amazon_clicks,
                amazon_ad_order_cnt + ebay_ad_order_cnt AS ad_order_qty,
                adv_fee_cny_fi  AS adv_fee_cny_fi,
                ad_order_sales_amt AS ad_sales_amt,
                order_cnt_30d  AS order_cnt_30d,
                first_inbound_date as first_inbound_date
         FROM ads.ads_alct_theory_profit_account_details_di
         WHERE dt >= date_trunc(date_sub(curdate(), interval 2 month), 'month')
         ) t1
     LEFT JOIN (
        -- 库存数据快照（取当前日期前2天的最新数据）
        SELECT
            sku,
            site_id,
            platform_id,
            MAX(stock_status) AS stock_status,
            MAX(available_qty) AS available_qty,
            SUM(IF(sku_plat_site_rn = 1, forecast_sales, 0)) AS forecast_sales,
            MAX(onway_stock_qty) AS onway_stock_qty,
            MAX(stock_turnover_day) AS stock_turnover_day,
            SUM(unqualified_qty_30d) AS unqualified_qty_30d,
            SUM(received_qty_30d) AS received_qty_30d,
            SUM(unqualified_qty_30d) / NULLIF(SUM(received_qty_30d), 0) AS unqualified_rate_30d,
            SUM(bad_goods_out_cost_cny_fi_30d) AS bad_goods_out_cost_cny_fi_30d,
            SUM(out_cost_cny_fi_30d) AS out_cost_cny_fi_30d,
            SUM(bad_goods_out_cost_cny_fi_30d) / NULLIF(SUM(out_cost_cny_fi_30d), 0) AS bad_goods_rate_overseaswarehouse_30d,
            SUM(bad_goods_out_cost_cny_fi_td) AS bad_goods_out_cost_cny_fi_td,
            SUM(out_cost_cny_fi_td) AS out_cost_cny_fi_td,
            SUM(bad_goods_out_cost_cny_fi_td) / NULLIF(SUM(out_cost_cny_fi_td), 0) AS bad_goods_rate_overseaswarehouse_td
        FROM ads.ads_alct_theory_profit_account_details_di
        WHERE dt = DATE_SUB(CURDATE(), INTERVAL 2 DAY)
        GROUP BY sku,
                 site_id,
                 platform_id
        ) t2 ON t1.sku = t2.sku AND t1.site_id = t2.site_id AND t1.platform_id = t2.platform_id
