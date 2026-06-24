with
    paid_bill as (
        select
            bill_code
          , bill_line_code
          , pay_time
          , apply_amount
          , po_order_code
          , original_order_code
          , biz_order_code
          , fee_code
        from dwd.dwd_fact_pcct_payment_apply_bill_line_df
        where
              date(pay_time) between date_sub(curdate(), interval 90 day) and date_sub(curdate(), interval 1 day)
          and bill_type = 1
          and pay_status = 3
          and fee_code in ('goods_pre_payment', 'goods_should_payment')
        )
  , delivery_period as (
        select distinct
            delivery_period
          , po_order_code
          , purchase_type_name
        from dwd.dwd_fact_pcct_purchase_demand_bill_df
        where
            demand_date >= '2023-08-26'
        )
  , po_order as (
        select *
        from
            (
                select
                    po_order_code
                  , max(paid_amount)                as paid_amount
                  , sum(line_total_amount_with_tax) as purchase_amt
                from dwd.dwd_fact_pcct_purchase_order_line_df
                group by
                    po_order_code
                ) as t
        where
            paid_amount >= purchase_amt
        )
  , stock_in_date as (
        select
            order_num
          , date(max(calc_time)) as stock_in_date
        from dwd.dwd_fact_ivct_ic_stock_in_order_di
        where
              stock_order_type_name = '采购入库'
          and qty > 0
        group by
            order_num
        )
  , result as (
        select
            pb.bill_code
          , pb.bill_line_code
          , pb.pay_time
          , pb.apply_amount
          , pb.po_order_code
          , pb.original_order_code
          , pb.biz_order_code
          , pb.fee_code
          , dp.delivery_period
          , si.stock_in_date
          , if(fee_code = 'goods_pre_payment', - dp.delivery_period,
               datediff(pay_time, stock_in_date)) as actual_account_period
        from
            paid_bill               as pb
                -- join po_order as po on po.po_order_code = pb.po_order_code
            join delivery_period    as dp
                on dp.po_order_code = pb.po_order_code and purchase_type_name <> '稳利宝采购'
            left join stock_in_date as si
                on si.order_num = pb.biz_order_code
        )
/*
select fee_code, sum(actual_account_period * apply_amount )/sum(apply_amount), sum(apply_amount)
from result
group by fee_code
;
 */
select
  sum(actual_account_period * apply_amount)/sum(apply_amount) as 账期x金额
from result
;
select
    bill_code                               付款申请单
  , bill_line_code                          付款申请子单
  , pay_time                                付款时间
  , apply_amount                            申请金额
  , po_order_code                           采购单号
  , original_order_code                     源单号
  , biz_order_code                          业务单号
  , fee_code                                费用类型
  , delivery_period                         账期
  , stock_in_date                           采购入库日期
  , actual_account_period                   实际账期
  , actual_account_period * apply_amount as 账期x金额
from result
order by
    pay_time
  , bill_line_code
  , po_order_code
;
-- 调整账期
with
    po_order as (
        select
            po_order_code
          , max(po_order_date)                                                       as po_order_date
          , max(b.pay_method_alias)                                                  as pay_method_alias
          , max(a.account_period_days)                                               as account_period_days
          , max(b.cash_rate)                                                         as cash_rate
          , sum(line_total_amount_with_tax)                                          as purchase_amt
          , max(if(po_order_line_status <> 60 and shelved_qty < purchase_qty, 1, 0)) as is_onway
          , sum((purchase_qty - shelved_qty) * purchase_price_with_tax)              as onway_amt
          , max(if(estimated_transitwarehouse_arrival_date < curdate(), curdate(),
                   estimated_transitwarehouse_arrival_date))                         as estimated_transitwarehouse_arrival_date
          , max(account_period_days)                                                 as po_account_period_days
          , max(po_order_line_status)                                                as po_order_line_status
            , max(shelved_qty) as shelved_qty
        , max(purchase_qty) as purchase_qty
        ,max(po_order_status) as po_order_status
        from
            dwd.dwd_fact_pcct_purchase_order_line_df as a
            join dwd.dwd_dim_pcct_pay_method_df      as b
                on a.pay_method_code = b.pay_code
        where
              po_order_date between '2026-05-01' and date_sub(curdate(), interval 1 day)
          and b.pay_method_alias <> '以销定结'
          and not (a.po_order_line_status in (30, 70) and a.received_qty = 0)
        group by
            po_order_code
        )
  , paid_bill as (
        select
            bill_code
          , bill_line_code
          , if(pay_time='1970-01-01 08:00:01', create_time, pay_time) as pay_time
          , if(apply_amount=0, should_pay_amount, apply_amount)                  as pay_amt
          , po_order_code
          , original_order_code
          , biz_order_code
          , fee_code
        from dwd.dwd_fact_pcct_payment_apply_bill_line_df
        where
              create_time >= '2026-05-01'
          and pay_status <> 2
          and bill_type = 1
          and fee_code in ('goods_pre_payment', 'goods_should_payment')
        )
  , stock_in_date as (
        select
            order_num
          , date(max(calc_time))    as stock_in_date
          , max(purchase_order_num) as po_order_code
        from dwd.dwd_fact_ivct_ic_stock_in_order_di
        where
              stock_order_type_name = '采购入库'
          and date(calc_time) >= '2026-05-01'
        group by
            order_num
        )
  , onway_stock as (
        select *
        from po_order
        where is_onway = 1
        )
  , result as (
        select
            po.po_order_date
          , po.po_order_code
          , po.pay_method_alias
          , pb.bill_code
          , pb.fee_code
          , pb.pay_time
          , pb.pay_amt
          , coalesce(si.stock_in_date, mi.min_stock_in_date,
                     po.estimated_transitwarehouse_arrival_date)                                                   as stock_in_date
          , datediff(pb.pay_time, coalesce(si.stock_in_date, mi.min_stock_in_date,
                                           po.estimated_transitwarehouse_arrival_date))                            as actual_account_period
          , po_account_period_days
          , purchase_amt
          , po_order_line_status
        from
            paid_bill               as pb
            join po_order           as po
                on po.po_order_code = pb.po_order_code
            left join stock_in_date as si
                on si.order_num = pb.original_order_code
            left join (
                select po_order_code, min(stock_in_date) as min_stock_in_date from stock_in_date group by po_order_code
                )                   as mi
                on mi.po_order_code = pb.po_order_code
        union all
        select
            po.po_order_date
          , po.po_order_code
          , po.pay_method_alias
          , sb.bill_code
          , 'goods_should_payment'                                 as fee_code
          , account_period_end_date                                as pay_time
          , (should_sett_line_amount - write_off_line_amount)      as pay_amt
          , original_order_date                                    as stock_in_date
          , datediff(account_period_end_date, original_order_date) as actual_account_period
          , po_account_period_days
          , purchase_amt
          , po_order_line_status
        from
            dwd.dwd_fact_pcct_purchase_settlement_bill_line_df as sb
            join po_order                                      as po
                on po.po_order_code = sb.po_order_code
        where
              sb.sett_type = 1
          and sb.biz_type = 1
          and (sb.paying_line_amount - sb.write_off_line_amount) <> 0
        union all
        select
            po_order_date
          , po_order_code
          , pay_method_alias
          , null                                                                                    as bill_code
          , 'goods_should_payment'                                                                  as fee_code
          , date_add(estimated_transitwarehouse_arrival_date, interval account_period_days + 5 day) as pay_time
          , onway_amt * (1.0 - cash_rate)                                                           as pay_amt
          , estimated_transitwarehouse_arrival_date                                                 as stock_in_date
          , account_period_days + 5                                                                 as actual_account_period
          , po_account_period_days
          , purchase_amt
          , po_order_line_status
        from onway_stock
        where
            cash_rate < 1.0
        union all
        select
            po_order_date
          , os.po_order_code
          , pay_method_alias
          , null                                                         as bill_code
          , 'goods_should_payment'                                       as fee_code
          , curdate()                                                    as pay_time
          , purchase_amt * cash_rate                                     as pay_amt
          , estimated_transitwarehouse_arrival_date                      as stock_in_date
          , datediff(curdate(), estimated_transitwarehouse_arrival_date) as actual_account_period
          , po_account_period_days
          , purchase_amt
          , po_order_line_status
        from
            onway_stock         as os
            left join paid_bill as pb
                on pb.po_order_code = os.po_order_code and pb.fee_code = 'goods_pre_payment'
        where
              cash_rate > 0
          and pb.po_order_code is null
        )
select
    rs.po_order_code
  , max(rs.purchase_amt)
  , sum(pay_amt)
  , count(1)
, max(po.is_onway)
  , max(shelved_qty) as shelved_qty
  , max(purchase_qty) as purchase_qty
,max(po.po_order_status) as po_order_status
from result as rs
join po_order as po on po.po_order_code = rs.po_order_code
group by
    rs.po_order_code
having
      max(rs.purchase_amt) - sum(pay_amt) > 1
;
/*select
    pay_method_alias
  , sum(pay_amt)                                                                  as pay_amt
  , sum(pay_amt * cast(actual_account_period as decimalv3(26, 8))) / sum(pay_amt) as avg_account_period
from result
where
    po_order_date = '2026-06-23'
group by
    pay_method_alias
order by
    pay_method_alias
;*/
select
    po_order_date
  , sum(pay_amt)                                                                  as pay_amt
  , sum(pay_amt * cast(actual_account_period as decimalv3(26, 8))) / sum(pay_amt) as avg_account_period
from result
group by
    po_order_date
order by
    po_order_date
;
select pay_time, pay_status, bill_status,apply_amount, po_order_code, fee_code
from dwd.dwd_fact_pcct_payment_apply_bill_line_df
where po_order_code = 'POB0012605190099'
;
select
    purchase_qty
  , received_qty
  , shelved_qty
  , po_order_line_status
, line_total_amount_with_tax
,paid_amount
from dwd.dwd_fact_pcct_purchase_order_line_df
where
    po_order_code = 'POB0012606090008'
;
select
    sett_type
  , bill_type
  , biz_type
  , should_sett_line_amount - write_off_line_amount
  , data_source_type
from dwd.dwd_fact_pcct_purchase_settlement_bill_line_df
where
    po_order_code = 'POB0012605190099'
;
select
    distinct stock_order_type_name, qty
from dwd.dwd_fact_ivct_ic_stock_in_order_di
where
      qty < 0
  and date(calc_time) >= '2026-05-01'
and order_num = 'CG260609000067'
;

