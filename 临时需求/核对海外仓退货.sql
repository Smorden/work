with
    bill_trans as (
        select
            warehouse_id
          , third_party_no
          , primary_standard_trans_code
          , bill_generated_time
          , calc_bill_time
          , currency_code
          , bill_amount
          , bill_calc_detail_str
          , warehouse_service_name
          , parcel_id
          , warehouse_cn_name
        from dwd.dwd_fact_lgct_tail_bill_transaction_di as bt
        where
              record_status = 1
          and sh_fee_item_name = '尾程杂费'
          and bill_calc_detail_str like '%退%'
          and bill_amount > 0
        )
  , stock_order as (
        select
            order_num_origin
          , group_concat(order_num) as order_num
        from
            (
                select
                    dt
                  , order_num
                  , sku
                  , qty
                  , order_num_origin
                  , warehouse_id
                from dwd.dwd_fact_ivct_ic_stock_in_order_di
                where
                      stock_order_type_name = '盘盈入库'
                  and reason_name = '海外仓退件'
                  and record_status = 1
                union all
                select
                    dt
                  , order_num
                  , sku
                  , qty
                  , order_num_origin
                  , warehouse_id
                from dwd.dwd_fact_ivct_ic_stock_out_order_di
                where
                      stock_order_type_name = '销售出库'
                  and qty < 0
                  and second_event_name = '海外仓-销退入库【关联到包裹号的】-实物域增加'
                  and record_status = 1
                ) as t
        group by
            order_num_origin
        )
select distinct
    bt.warehouse_service_name               仓库服务商
  , bt.warehouse_cn_name                    仓库名
  , bt.primary_standard_trans_code          流水号
  , bt.calc_bill_time                       账单时间
  , bt.bill_generated_time                  费用发生时间
  , bt.third_party_no                       第三方单号
  , bt.parcel_id                            发货单号
  , bt.currency_code                        币种
  , bt.bill_amount                          账单金额
  , bt.bill_calc_detail_str                 账单明细
  , if(so.order_num is null, '否', '是') as 库存中心是否处理
  , so.order_num                            库存中心单号
from
    bill_trans            as bt
    left join stock_order as so
        on so.order_num_origin = if(bt.warehouse_service_name = '4PX', bt.parcel_id, bt.third_party_no)
order by
    bt.calc_bill_time
  , bt.warehouse_service_name
  , bt.warehouse_cn_name
;