drop table temp.temp_dwd_dim_tcct_ykd_handing_fee_df;
create table if not exists temp.temp_dwd_dim_tcct_ykd_handing_fee_df
(
    cal_type                tinyint comment '1:单个si，2多个si按包裹附加',
    country                 varchar(50) comment "国家",
    start_weight            int comment "起始重量",
    end_weight              int comment "结束重量",
    outstockfee             decimal(26, 8) comment "出库处理费",
    outstockfee_additionfee decimal(26, 8) comment "出库处理费_续重费",
    instockfee              decimal(26, 8) comment "入库处理费",
    instock_additionfee     decimal(26, 8) comment "入库出库费_续重费",
    currency                varchar(10) comment "报价币种",
    etl_create_time         datetime default CURRENT_TIMESTAMP COMMENT "etl执行时间"
) Duplicate KEY(cal_type,country)
comment "ykd处理费报价-si"
DISTRIBUTED BY HASH(country) BUCKETS 1
PROPERTIES("light_schema_change" = "true")
;

with stock_out_order as (
    select dt, order_num, sku, order_num_origin as parcel_id
             , warehouse_id, qty
    from dwd.dwd_fact_ivct_ic_stock_out_order_di
    where dt between  '2026-03-01' and '2026-04-30'
        AND record_status = 1
      and stock_order_type_name = '销售出库'
      and qty > 0
      and order_status = 3
    )
   , cancel_parcel as (
       select parcel_id
       from dwd.dwd_dim_tcct_oms_parcel_di
       where dt >= '2026-03-01'
       and cancel_status = 2
    )
    , parcel_sku as (
        select
            so.dt
          , so.parcel_id
          , so.sku
          , so.qty as quantity
          , wh.warehouse_en_name
             , wh.warehouse_cn_name
          , if(left(warehouse_en_name, 2) = 'US', left(warehouse_en_name, 3), left(warehouse_en_name, 2)) AS country
        from stock_out_order as so
            join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = so.warehouse_id
            and wh.warehouse_service_name = 'YKD'
        left join cancel_parcel as cp on cp.parcel_id = so.parcel_id
        where cp.parcel_id is null
        )
  , dim_sku as (
        select dt, sku, weight as sku_weight from dwd.dwd_dim_sku_ds where dt between '2026-03-01' and '2026-04-30'
        )
  , parcel_sku_weight as (
        select
            ps.*
          , sku.sku_weight
          , sum(quantity) over (partition by ps.parcel_id)                  as parcel_quantity
          , sum(sku.sku_weight * quantity) over (partition by ps.parcel_id) as parcel_weight
          , row_number() over (partition by ps.parcel_id order by ps.sku)      as rn
        , os.platform_outbound_order_no
        , os.platform_outbound_complete_time
        , os.platform_outbound_cancel_time
        from
            parcel_sku   as ps
            join dim_sku as sku
                on sku.dt = ps.dt and sku.sku = ps.sku
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
        ,case a.country when '英国' then 0.75
        when '德国' then 0.85
        else 1 end as discount_rate
        from
            temp.temp_dwd_dim_tcct_ykd_handing_fee_df as a
            left join dwd.dwd_dim_country_df          as b
                on b.country_cn_name = a.country
        )
  , exchange_rate as (
        select
            currency_code
          , start_date
          , end_date
          , currency_rate
        from dwd.dwd_dim_exchange_rate_df
        where
            start_date between '2026-03-01' and '2026-04-30'
        )
  , result as (
        select
            sw.*
          , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
                   (si.outstockfee_additionfee) + si.outstockfee,
                   0)*quantity*si.discount_rate                                    as handle_fee_si
          , ifnull(pc.outstockfee*pc.discount_rate , 0)                   as handle_fee_parcel
          , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
                   (si.outstockfee_additionfee) + si.outstockfee, 0)*quantity *
            er.currency_rate *si.discount_rate                            as handle_fee_si_cny
          , ifnull(pc.outstockfee*pc.discount_rate, 0) * er.currency_rate  as handle_fee_parcel_cny
            , si.currency
        from
            parcel_sku_weight          as sw
            left join handle_fee_price as si
                on si.country = sw.country and sw.sku_weight > si.start_weight and sw.sku_weight <= si.end_weight and
                   si.cal_type = 1
            left join handle_fee_price as pc
                on pc.country = sw.country and sw.parcel_weight > pc.start_weight and
                   sw.parcel_weight <= pc.end_weight and pc.cal_type = 2 and sw.parcel_quantity > 1
            left join exchange_rate    as er
                on sw.dt between er.start_date and er.end_date and er.currency_code = si.currency
        )
select
    dt 出库日期, parcel_id 发货单号
     , platform_outbound_order_no 三方出库单号
     , sku, quantity 数量
     , sku_weight sku重量g
     , parcel_quantity 包裹数量
     , parcel_weight 包裹重量g
    , warehouse_cn_name 仓库
     , currency 币种
     , handle_fee_si  as 出库处理费
    , handle_fee_parcel as 多件包裹处理费
     , handle_fee_si + handle_fee_parcel as 处理费原币
    , handle_fee_si_cny + handle_fee_parcel_cny as 处理费rmb
from result
where dt between  '2026-04-01' and '2026-04-30'
order by dt, parcel_id, sku
;
-- 入库处理费
with stock_out_order as (
    select dt, order_num, sku, order_num_origin
             , warehouse_id, qty, stock_order_type_name
    from dwd.dwd_fact_ivct_ic_stock_in_order_di
    where dt between  '2026-03-01' and '2026-04-30'
      AND record_status = 1
      and warehouse_id <> 1
      and qty > 0
      and order_status = 3
    )
  , parcel_sku as (
    select
        so.dt
      , so.order_num_origin
      , so.sku
      , so.qty as quantity
      , wh.warehouse_en_name
      , wh.warehouse_cn_name
      , if(left(warehouse_en_name, 2) = 'US', left(warehouse_en_name, 3), left(warehouse_en_name, 2)) AS country
        , so.order_num, so.stock_order_type_name
    from stock_out_order as so
         join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = so.warehouse_id
            and wh.warehouse_service_name = 'YKD'
    )
  , dim_sku as (
    select dt, sku, weight as sku_weight from dwd.dwd_dim_sku_ds where dt between '2026-03-01' and '2026-04-30'
    )
  , parcel_sku_weight as (
    select
        ps.*
      , sku.sku_weight
    from
        parcel_sku   as ps
        join dim_sku as sku
            on sku.dt = ps.dt and sku.sku = ps.sku
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
    from
        temp.temp_dwd_dim_tcct_ykd_handing_fee_df as a
        left join dwd.dwd_dim_country_df          as b
            on b.country_cn_name = a.country
    )
  , exchange_rate as (
    select
        currency_code
      , start_date
      , end_date
      , currency_rate
    from dwd.dwd_dim_exchange_rate_df
    where
        start_date between '2026-03-01' and '2026-04-30'
    )
  , result as (
    select
        sw.*
      , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
               (si.instock_additionfee) + si.instockfee,
               0)*quantity                                    as handle_fee_si
      , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
               (si.instock_additionfee) + si.instockfee, 0)*quantity *
        er.currency_rate                             as handle_fee_si_cny
      , si.currency
    from
        parcel_sku_weight          as sw
        left join handle_fee_price as si
            on si.country = sw.country and sw.sku_weight > si.start_weight and sw.sku_weight <= si.end_weight and
               si.cal_type = 1
        left join exchange_rate    as er
            on sw.dt between er.start_date and er.end_date and er.currency_code = si.currency
    )
select
    dt 入库日期, order_num 入库单号
               , order_num_origin 源单号
     ,stock_order_type_name 入库类型
               , sku, quantity 数量
               , sku_weight sku重量g
               , warehouse_cn_name 仓库
               , currency 币种
               , handle_fee_si  as 入库处理费原币
               , handle_fee_si_cny as 入库处理费rmb
from result
-- where dt between  '2026-04-01' and '2026-04-30'
order by dt,order_num,sku
;

