drop table temp.temp_dwd_dim_tcct_tail_handing_fee_df;
create table if not exists temp.temp_dwd_dim_tcct_tail_handing_fee_df
(
    dt date comment "报价日期",
    cal_type                tinyint comment '1:单个si，2多个si按包裹附加,3入库卸货费，按体积计算，入库处理费_续重费 * 体积',
    country                 varchar(50) comment "国家",
    start_weight            int comment "起始重量",
    end_weight              int comment "结束重量",
    outstockfee             decimal(26, 8) comment "出库处理费",
    outstockfee_additionfee decimal(26, 8) comment "出库处理费_续重费",
    instockfee              decimal(26, 8) comment "入库处理费",
    instock_additionfee     decimal(26, 8) comment "入库处理费_续重费",
    currency                varchar(10) comment "报价币种",
    warehouse_service_name varchar(50) comment "仓库服务名称",
    etl_create_time         datetime default CURRENT_TIMESTAMP COMMENT "etl执行时间"
) Duplicate KEY(dt, cal_type,country)
comment "尾程处理费报价-si"
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES("light_schema_change" = "true")
;
insert into temp.temp_dwd_dim_tcct_tail_handing_fee_df
    (
      dt
,cal_type
,country
,start_weight
,end_weight
,outstockfee
,outstockfee_additionfee
,instockfee
,instock_additionfee
,currency
    ,warehouse_service_name
    )
select dt
  ,cal_type
  ,country
  ,start_weight
  ,end_weight
  ,outstockfee
  ,outstockfee_additionfee
  ,instockfee
  ,instock_additionfee
  ,currency
  ,'YKD' as warehouse_service_name
    from temp.temp_dwd_dim_tcct_ykd_handing_fee_df
;
SELECT *
FROM temp.temp_dwd_dim_tcct_tail_handing_fee_df
;
select calc_bill_time, bill_generated_time, parcel_id, third_party_no, currency_code, bill_amount
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where warehouse_service_name = 'YKD'
and dt between '2026-04-01' and '2026-04-30'
and sh_fee_item_name = '海外仓处理费'
and bill_calc_detail_str not like '入库操作费%'
and record_status = 1
;
-- 2025-08-19 14:15:04
select parcel_id
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where warehouse_service_name = 'YKD'
  and dt between '2026-04-01' and '2026-04-30'
  and sh_fee_item_name = '海外仓处理费'
  and bill_calc_detail_str not like '入库操作费%'
  and record_status = 1
and parcel_id is not null
group by parcel_id
having count(1) > 1
;
select count(1)
from temp.temp_dwd_dim_tcct_ykd_handing_fee_df
where dt = '2026-04-01'
;
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
      and dt between '2026-06-01' and '2026-06-30'
      and sh_fee_item_name = '海外仓处理费'
      and bill_calc_detail_str not like '入库操作费%'
      and record_status = 1
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
        where dt between '2026-06-01' and '2026-06-30'
        )
   ,product_info as (
       select dt, goods_code, sku_weight
       from dwd.dwd_dim_tcct_ykd_product_info_ds
       where dt between '2026-06-01' and '2026-06-30'
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
        ,case a.country when '英国' then 0.75
        when '德国' then 0.85
        else 0.85 end as discount_rate
        , dt
        from
            temp.temp_dwd_dim_tcct_ykd_handing_fee_df as a
            left join dwd.dwd_dim_country_df          as b
                on b.country_cn_name = a.country
        where a.dt = '2026-04-01'
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
/*select rs.parcel_id 发货单号, sku, goods_code 货品, quantity 数量, origin_sku_weight sku重量, goods_weight 货品重量
from result as rs
join temp.temp_parcel_id as tp on tp.parcel_id = rs.parcel_id
order by rs.parcel_id, sku
;*/
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
-- where tb.bill_amount - FLOOR(ifnull(rs.handle_fee_out, 0) * POW(10, 2) + 0.5) / POW(10, 2) > 0.01
order by tb.calc_bill_time, tb.bill_generated_time, tb.parcel_id
    ;
select *
from dwd.dwd_dim_tcct_ykd_product_info_ds
where dt = '2026-06-23'
and goods_code = 'OTJSHHUAJIA002'
;
select *
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where bill_amount > 0
and warehouse_service_name = 'YKD'
and record_status = 1
and dt between '2026-03-01' and '2026-03-31'
and sh_fee_item_name = '海外仓处理费'
and bill_calc_detail_str like '入库操作费%'
;
/*select
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
;*/
-- 入库处理费
with stock_out_order as (
    select dt, order_num, sku, order_num_origin
             , warehouse_id, qty, stock_order_type_name
    from dwd.dwd_fact_ivct_ic_stock_in_order_di
    where dt >= '2025-01-01'
      AND record_status = 1
      and warehouse_id <> 1
      and qty > 0
      and order_status = 3
    )
   , third_inbound_order as (
       select ship_no, inbound_no
       from dwd.dwd_dim_ivct_ship_info_ds
       where dt = date_sub(curdate(), interval 1 day)
    )
   , transaction_bill as (
    select dt, calc_bill_time, bill_generated_time, third_party_no, bill_amount, currency_code
                         ,warehouse_id
    from dwd.dwd_fact_lgct_tail_bill_transaction_di
    where warehouse_service_name = 'YKD'
      and record_status = 1
      and dt between '2026-03-01' and '2026-03-31'
      and sh_fee_item_name = '海外仓处理费'
      and bill_calc_detail_str like '入库操作费%'
    )
  , parcel_sku as (
    select
        tb.dt
      , so.order_num_origin
      , so.sku
      , so.qty as quantity
      , wh.warehouse_en_name
      , wh.warehouse_cn_name
      , if(left(warehouse_en_name, 2) = 'US', left(warehouse_en_name, 3), left(warehouse_en_name, 2)) AS country
        , so.order_num, so.stock_order_type_name
    , ti.inbound_no
    , tb.third_party_no
    , ti.ship_no
    from stock_out_order as so
         join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = so.warehouse_id
            and wh.warehouse_service_name = 'YKD'
    left join third_inbound_order as ti on ti.ship_no = so.order_num_origin
    join transaction_bill as tb on tb.third_party_no = ifnull(ti.inbound_no, so.order_num_origin)
    )
  , dim_sku as (
    select dt, sku, weight as sku_weight
    from dwd.dwd_dim_sku_ds
    where dt between '2026-03-01' and '2026-03-31'
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
    select dt
       , cal_type
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
                      else 0.85 end as discount_rate
    from
        temp.temp_dwd_dim_tcct_ykd_handing_fee_df as a
        left join dwd.dwd_dim_country_df          as b
            on b.country_cn_name = a.country
    )
/*  , exchange_rate as (
    select
        currency_code
      , start_date
      , end_date
      , currency_rate
    from dwd.dwd_dim_exchange_rate_df
    where
        start_date between '2026-02-01' and '2026-05-31'
    )*/
  , result as (
    select
        sw.*
      , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
               (si.instock_additionfee) + si.instockfee,
               0)*quantity*si.discount_rate                                    as handle_fee_si
/*      , ifnull(ceil((sw.sku_weight - si.start_weight) / 1000) *
               (si.instock_additionfee) + si.instockfee, 0)*quantity*si.discount_rate *
        er.currency_rate                             as handle_fee_si_cny*/
      , si.currency
    from
        parcel_sku_weight          as sw
        left join handle_fee_price as si
            on si.country = sw.country and sw.sku_weight > si.start_weight and sw.sku_weight <= si.end_weight and
               si.cal_type = 1
            and si.dt = date_trunc(sw.dt, 'month')
/*        left join exchange_rate    as er
            on sw.dt between er.start_date and er.end_date and er.currency_code = si.currency*/
    )
select tb.calc_bill_time as 记账时间
, tb.bill_generated_time as 费用发生时间, tb.third_party_no as 三方单号
     ,rs.order_num as 入库单号
     ,rs.ship_no as 调拨单号
, tb.currency_code 币种
, tb.bill_amount 账单费用
, ifnull(rs.handle_fee, 0) as 计算费用
from transaction_bill as tb
     left join (select third_party_no, sum(handle_fee_si) as handle_fee
                    , group_concat(distinct order_num) as order_num
                , max(ship_no) as ship_no
                from result
                group by third_party_no
                ) as rs on rs.third_party_no = tb.third_party_no
-- where rs.third_party_no is null
order by tb.calc_bill_time, tb.bill_generated_time, tb.third_party_no
;
select *
from dwd.dwd_fact_ivct_ic_stock_in_order_di
where order_num_origin = 'RVG11497-260310-0003'
;
select *
from dwd.dwd_fact_ivct_ic_stock_out_order_di
where order_num_origin = ' RVG11497-260326-0021'
;
select * from dwd.dwd_dim_ivct_ship_info_ds
where dt = '2026-06-10'
and inbound_no = 'RVG11497-260310-0003'
;
select *
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where third_party_no = 'RVG11497-251220-0005'
;
select distinct left(bill_calc_detail_str, 5)
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where warehouse_service_name = '4PX'
  and record_status = 1
  and dt between '2026-04-01' and '2026-04-30'
  -- and sh_fee_item_name = '海外仓处理费'
  and left(bill_calc_detail_str, 2) in ('入库','出库')
;
select distinct warehouse_cn_name
from dwd.dwd_fact_lgct_tail_bill_transaction_di
where warehouse_service_name = '4PX'
  and record_status = 1
  and dt between '2026-06-01' and '2026-06-30'
      -- and sh_fee_item_name = '海外仓处理费'
  and left(bill_calc_detail_str, 2) = '出库'
;
select dt, order_num, sku, order_num_origin
         , warehouse_id, qty, stock_order_type_name
from dwd.dwd_fact_ivct_ic_stock_in_order_di
where dt >= '2026-04-01'
  AND record_status = 1
  and warehouse_id <> 1
  and qty > 0
  and order_status = 3
and warehouse_id in (select warehouse_id from dwd.dwd_dim_warehouse_df where warehouse_service_name = '4PX')
AND stock_order_type_name= '调拨入库'
;