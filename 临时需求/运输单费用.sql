select
    a.transport_order_no as `运输单号`
    ,a.transport_type_name as `运输类型`
    ,a.transport_method_name as `运输方式`
    ,a.warehouse_cn_name as `目的仓`
    ,a.qty as `数量`
    ,a.box_qty as `箱数`
    ,a.total_volume as `体积`
    ,a.total_weight as `重量`
    ,a.real_declare_amount as `实际申报货值（USD)`
    ,a.actual_order_date as `实际下单日期`
    ,a.actual_sailing_date as `实际开船日期`
    ,b.sh_fee_item_name as `费用项目`
    ,b.currency_code as `币种`
    ,b.estimate_amount as `预估费用金额`
    ,b.settlement_amount as `实际结算费用金额`
from ads.ads_finebi_transport_order_details_df as a
left join dws.dws_lgct_first_trip_estimate_and_settlement_amt_df as b on b.transport_order_no = a.transport_order_no
where a.actual_order_date >= '2026-02-01'
and a.logistics_status <> 'cancelled'
order by a.actual_order_date, a.transport_order_no, b.sh_fee_item_name
;