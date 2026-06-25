create table if not exists temp.temp_dwd_dim_tcct_tail_handing_fee_df
(
    dt date comment "报价日期",
    warehouse_service_name varchar(50) comment "仓库服务名称",
    cal_type                tinyint comment '1:单个si，2多个si按包裹附加,3入库卸货费，按体积计算，入库处理费_续重费 * 体积',
    country                 varchar(50) comment "国家",
    start_weight            int comment "起始重量",
    end_weight              int comment "结束重量",
    outstockfee             decimal(26, 8) comment "出库处理费",
    outstockfee_additionfee decimal(26, 8) comment "出库处理费_续重费",
    instockfee              decimal(26, 8) comment "入库处理费",
    instock_additionfee     decimal(26, 8) comment "入库处理费_续重费",
    currency                varchar(10) comment "报价币种",
    etl_create_time         datetime default CURRENT_TIMESTAMP COMMENT "etl执行时间"
) Duplicate KEY(dt, cal_type,country)
comment "尾程处理费报价-si"
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES("light_schema_change" = "true")
;
出库包装费
;
select distinct left(bill_calc_detail_str, 5)
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where warehouse_service_name = '4PX'
  and dt between '2026-05-01' and '2026-05-31'
  and record_status = 1
and bill_calc_detail_str like '出库%'
;
-- 出库操作费
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
       where dt >= '2026-04-01'
       and cancel_status = 2
    )*/
  , transaction_bill as (
    select dt, calc_bill_time, bill_generated_time, parcel_id, third_party_no, currency_code, bill_amount,warehouse_cn_name
    from dwd.dwd_fact_lgct_tail_bill_transaction_di
    where warehouse_service_name = '4PX'
      and dt between '2026-05-01' and '2026-05-31'
      and sh_fee_item_name = '海外仓处理费'
      and left(bill_calc_detail_str, 2) = '出库'
      and record_status = 1
    )
  , parcel_sku as (
    select
        tb.dt
      , so.parcel_id
      , so.sku
      , so.qty as quantity
      , wh.warehouse_en_name
      , wh.warehouse_cn_name
      , if(left(warehouse_en_name, 2) = 'US', left(warehouse_en_name, 3), wh.warehouse_location_country) AS country
    ,so.goods_code
    from stock_out_order as so
         join transaction_bill as tb on tb.parcel_id = so.parcel_id
         join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = so.warehouse_id
            and wh.warehouse_service_name = '4PX'
    -- left join cancel_parcel as cp on cp.parcel_id = so.parcel_id
    -- where cp.parcel_id is null
    )
  , dim_sku as (
    select dt, sku, weight as sku_weight
    from dwd.dwd_dim_sku_ds
    where dt between '2026-05-01' and '2026-05-31'
    )
   , product_info as (
       select dt, sku, goods_code, sku_weight
       from dwd.dwd_dim_tcct_dsf_product_info_ds
       where dt between '2026-05-01' and '2026-05-31'
    )
  , parcel_sku_weight as (
    select
        ps.*
      , ifnull(pi.sku_weight, sku.sku_weight) as sku_weight
      , sum(quantity) over (partition by ps.parcel_id)                  as parcel_quantity
      , sum(ifnull(pi.sku_weight, sku.sku_weight) * quantity) over (partition by ps.parcel_id) as parcel_weight
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
            else b.country_short_name
            end as country
      , start_weight
      , end_weight
      , outstockfee
      , outstockfee_additionfee
      , instockfee
      , instock_additionfee
      , currency
      ,1.0 as discount_rate
      , dt
    from
        temp.temp_dwd_dim_tcct_tail_handing_fee_df as a
        left join dwd.dwd_dim_country_df          as b
            on b.country_cn_name = a.country
        where a.dt = '2026-04-01'
            and a.warehouse_service_name = '4PX'
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
-- select * from result where parcel_id = 'FH177487866578769715';
select tb.calc_bill_time as 记账时间, tb.bill_generated_time as 费用发生时间, tb.parcel_id 发货单号
                                    , tb.third_party_no 三方单号
                                    , tb.currency_code 币种
                                    , tb.bill_amount 账单费用
                                    , ifnull(rs.handle_fee_out, 0) as 计算费用
,tb.warehouse_cn_name 仓库
from transaction_bill as tb
     left join (select parcel_id, sum(handle_fee_si ) as handle_fee_out
                from result
                group by parcel_id
                ) as rs on rs.parcel_id = tb.parcel_id
order by tb.calc_bill_time, tb.bill_generated_time, tb.parcel_id
;