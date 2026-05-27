with paid_bill as (
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
        and fee_code in ('goods_pre_payment','goods_should_payment')
    )
, delivery_period as (
    select distinct delivery_period, po_order_code, purchase_type_name
    from dwd.dwd_fact_pcct_purchase_demand_bill_df
    where demand_date >= '2023-08-26'
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
        group by po_order_code
        ) as t
    where paid_amount >= purchase_amt
    )
, stock_in_date as (
    select order_num, date(max(calc_time)) as stock_in_date
    from dwd.dwd_fact_ivct_ic_stock_in_order_di
    where stock_order_type_name = '采购入库'
        and qty > 0
    group by order_num
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
           datediff(pay_time, stock_in_date )) as actual_account_period
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
    bill_code 付款申请单
  , bill_line_code 付款申请子单
  , pay_time 付款时间
  , apply_amount 申请金额
  , po_order_code 采购单号
  , original_order_code 源单号
  , biz_order_code 业务单号
  , fee_code 费用类型
,delivery_period 账期
,stock_in_date 采购入库日期
,actual_account_period 实际账期
,actual_account_period * apply_amount as 账期x金额
from result
order by pay_time, bill_line_code, po_order_code
;