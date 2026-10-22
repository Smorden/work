-- 洲际(zj)处理费报价-si  入库/出库操作费
-- 来源：D:\转账凭证\洲际报价.xls (Sheet1)
-- 国家统一填：德国    币种：EUR
-- 重量档位由报价单的 KG 换算为克（与 temp_dwd_dim_tcct_ykd_handing_fee_df 口径一致）
-- cal_type=1 单件出入库操作费；cal_type=2 复合订单操作费（多件包裹附加）
-- 续重费：>40KG 部分按 KG 计费，沿用源表语义
--   基础费沿用 30<X≤40KG 档，续重单价放 *_additionfee
--   计算式：ceil((sku_weight - start_weight)/1000) * additionfee + fee

-- 注意：Doris 客户端需带 --default-character-set=utf8mb4，否则中文注释/国家值会写成乱码
SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS `temp`.`temp_dwd_dim_tcct_zj_handing_fee_df` (
  `cal_type` tinyint(4) NULL COMMENT '1:单个si，2多个si按包裹附加',
  `country` varchar(50) NULL COMMENT '国家',
  `start_weight` int(11) NULL COMMENT '起始重量(克)',
  `end_weight` int(11) NULL COMMENT '结束重量(克)',
  `outstockfee` decimal(26, 8) NULL COMMENT '出库处理费',
  `outstockfee_additionfee` decimal(26, 8) NULL COMMENT '出库处理费_续重费',
  `instockfee` decimal(26, 8) NULL COMMENT '入库处理费',
  `instock_additionfee` decimal(26, 8) NULL COMMENT '入库处理费_续重费',
  `currency` varchar(10) NULL COMMENT '报价币种',
  `etl_create_time` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'etl执行时间'
) ENGINE=OLAP
DUPLICATE KEY(`cal_type`, `country`)
COMMENT '洲际处理费报价-si'
DISTRIBUTED BY HASH(`country`) BUCKETS 1
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "light_schema_change" = "true",
  "disable_auto_compaction" = "false"
);

TRUNCATE TABLE temp.temp_dwd_dim_tcct_zj_handing_fee_df;

INSERT INTO temp.temp_dwd_dim_tcct_zj_handing_fee_df
  (cal_type, country, start_weight, end_weight,
   outstockfee, outstockfee_additionfee, instockfee, instock_additionfee, currency)
VALUES
-- ==================== cal_type=1 单件出入库操作费（德国） ====================
-- 0<X≤1 KG
(1, '德国',       0,  1000, 0.20, 0.00, 0.20, 0.00, 'EUR'),
-- 1<X≤3 KG
(1, '德国',    1000,  3000, 0.35, 0.00, 0.22, 0.00, 'EUR'),
-- 3<X≤5 KG
(1, '德国',    3000,  5000, 0.45, 0.00, 0.25, 0.00, 'EUR'),
-- 5<X≤10 KG
(1, '德国',    5000, 10000, 0.75, 0.00, 0.40, 0.00, 'EUR'),
-- 10<X≤15 KG
(1, '德国',   10000, 15000, 1.20, 0.00, 0.60, 0.00, 'EUR'),
-- 15<X≤20 KG
(1, '德国',   15000, 20000, 1.40, 0.00, 0.80, 0.00, 'EUR'),
-- 20<X≤30 KG
(1, '德国',   20000, 30000, 1.60, 0.00, 1.00, 0.00, 'EUR'),
-- 30<X≤40 KG
(1, '德国',   30000, 40000, 2.00, 0.00, 2.50, 0.00, 'EUR'),
-- 续重费 X>40KG（基础费沿用 30-40KG 档）
(1, '德国',   40000, 999999999, 2.00, 0.10, 2.50, 0.12, 'EUR'),
-- ==================== cal_type=2 复合订单操作费（包裹内件数>1 时收取，0.5/SKU个数） ====================
(2, '德国',       0, 999999999, 0.50, 0.00, 0.00, 0.00, 'EUR');

SELECT * FROM temp.temp_dwd_dim_tcct_zj_handing_fee_df ORDER BY cal_type, start_weight;


with stock_out_order as (
    select dt, order_num, sku, order_num_origin as parcel_id
             , warehouse_id, qty, goods_code
    from dwd.dwd_fact_ivct_ic_stock_out_order_di
    where dt >=  '2026-01-01'
      AND record_status = 1
      and stock_order_type_name = '销售出库'
      and qty > 0
      and order_status = 3
    )
/*   , cancel_parcel as (
       select parcel_id
       from dwd.dwd_dim_tcct_oms_parcel_di
       where dt >= '2026-03-01'
       and cancel_status = 2
    )*/
  , transaction_bill as (
    select dt, calc_bill_time, bill_generated_time, parcel_id, third_party_no, currency_code, bill_amount, warehouse_cn_name
    from dwd.dwd_fact_lgct_tail_bill_transaction_di
    where warehouse_service_name = 'YKD'
      and dt between '2026-09-01' and '2026-09-30'
      and sh_fee_item_name = '海外仓处理费'
      and bill_calc_detail_str not like '入库操作费%'
      and record_status = 1
      and warehouse_cn_name like '%德国%'
    )
  , parcel_sku as (
    select
        tb.dt
      , so.parcel_id
      , so.sku
      , so.goods_code
      , so.qty as quantity
      , wh.warehouse_en_name
      , wh.warehouse_cn_name
      , if(left(warehouse_en_name, 2) = 'US', left(warehouse_en_name, 3), left(warehouse_en_name, 2)) AS country
    from stock_out_order as so
         join transaction_bill as tb on tb.parcel_id = so.parcel_id
         join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = so.warehouse_id
            and wh.warehouse_service_name = 'YKD'
    -- left join cancel_parcel as cp on cp.parcel_id = so.parcel_id
    -- where cp.parcel_id is null
    )
  , dim_sku as (
    select dt, sku, weight as sku_weight
    from dwd.dwd_dim_sku_ds
    where dt between '2026-09-01' and '2026-09-30'
    )
  ,product_info as (
    select dt, goods_code, sku_weight
    from dwd.dwd_dim_tcct_ykd_product_info_ds
    where dt between '2026-09-01' and '2026-09-30'
    )
  , parcel_sku_weight as (
    select
        ps.*
      , ifnull(pi.sku_weight,sku.sku_weight) as sku_weight
      ,pi.sku_weight as goods_weight
      ,sku.sku_weight as origin_sku_weight
      , sum(quantity) over (partition by ps.parcel_id)                  as parcel_quantity
      , sum(ifnull(pi.sku_weight,sku.sku_weight) * quantity) over (partition by ps.parcel_id) as parcel_weight
      , row_number() over (partition by ps.parcel_id order by ps.sku)      as rn
      , os.platform_outbound_order_no
      , os.platform_outbound_complete_time
      , os.platform_outbound_cancel_time
    from
        parcel_sku   as ps
        join dim_sku as sku
            on sku.dt = ps.dt and sku.sku = ps.sku
        left join product_info as pi on pi.dt = ps.dt and pi.goods_code = ps.goods_code
        left join dwd.dwd_fact_tcct_parcel_outbound_summary_di as os on os.parcel_id = ps.parcel_id
    )
  , handle_fee_price AS (
    select
        cal_type
      , case a.country
            when '新泽西'
                then 'USE'
            WHEN '加州'
                then 'USW'
            when '英国'
                then 'UK'
            else b.country_short_name
            end as country
      , start_weight
      , end_weight
      , outstockfee
      , outstockfee_additionfee
      , instockfee
      , instock_additionfee
      , currency
      ,1 as discount_rate
    from
        temp.temp_dwd_dim_tcct_zj_handing_fee_df as a
        left join dwd.dwd_dim_country_df          as b
            on b.country_cn_name = a.country
    )
  , result as (
    select
        sw.*
      , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
               (si.outstockfee_additionfee) + si.outstockfee,
               0)*quantity*si.discount_rate                                    as handle_fee_si
      , ifnull(pc.outstockfee*pc.discount_rate , 0)                   as handle_fee_parcel
/*          , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
                   (si.outstockfee_additionfee) + si.outstockfee, 0)*quantity *
            er.currency_rate *si.discount_rate                            as handle_fee_si_cny
          , ifnull(pc.outstockfee*pc.discount_rate, 0) * er.currency_rate  as handle_fee_parcel_cny*/
      , si.currency
    from
        parcel_sku_weight          as sw
        left join handle_fee_price as si
            on si.country = sw.country and sw.sku_weight > si.start_weight and sw.sku_weight <= si.end_weight and
               si.cal_type = 1
            -- and si.dt = date_trunc(sw.dt, 'month')
        left join handle_fee_price as pc
            on pc.country = sw.country and sw.parcel_weight > pc.start_weight and
               sw.parcel_weight <= pc.end_weight and pc.cal_type = 2 and sw.parcel_quantity > 1
    -- and pc.dt = date_trunc(sw.dt, 'month')
/*            left join exchange_rate    as er
                on sw.dt between er.start_date and er.end_date and er.currency_code = si.currency
        */
    )
select tb.calc_bill_time as 记账时间, tb.bill_generated_time as 费用发生时间, tb.parcel_id 发货单号
                                    , tb.third_party_no 三方单号
                                    , tb.currency_code 币种
                                    , tb.bill_amount 账单费用
                                    , cast(FLOOR(ifnull(rs.handle_fee_out, 0) * POW(10, 2) + 0.5) / POW(10, 2) as decimal(10,2)) as 计算费用
                                    ,tb.bill_amount - cast(FLOOR(ifnull(rs.handle_fee_out, 0) * POW(10, 2) + 0.5) / POW(10, 2) as decimal(10,2)) as 差异金额
                                    , tb.warehouse_cn_name as 仓库
                                    , rs.goods_code 货品, rs.quantity 数量, rs.sku_weight 重量
from transaction_bill as tb
     left join (select parcel_id, handle_fee_si + handle_fee_parcel as handle_fee_out
                                , goods_code, quantity,sku_weight
                from result
                ) as rs on rs.parcel_id = tb.parcel_id
-- where tb.warehouse_cn_name like '%法国%'
-- where tb.bill_amount - FLOOR(ifnull(rs.handle_fee_out, 0) * POW(10, 2) + 0.5) / POW(10, 2) > 0.01
order by tb.calc_bill_time, tb.bill_generated_time, tb.parcel_id
;