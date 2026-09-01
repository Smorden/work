select po_order_code 采购单, bill_code 付款单, apply_amount 金额,case bill_status
                                                                     when 11 then '草稿'
                                                                     when 12 then '审核中'
                                                                     when 13 then '已提现'
                                                                     when 14 then '审核驳回'
                                                                     when 15 then '已作废'
                                                                     when 16 then '未提现'
                                                                     when 21 then '待确认'
                                                                     when 22 then '已退款'
                                                                     when 23 then '已作废'
                                                                     when 24 then '已驳回'
                                                                     else '未知'
                                                                     end as 状态
from dwd.dwd_fact_pcct_payment_apply_bill_line_df
where po_order_code in (
                        'POB0012606110011',
                        'POB0012606110021'
    )
order by po_order_code, bill_code
;