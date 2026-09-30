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
select
    b.bill_code as 付款单号,
    case b.bill_status
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
            end 单据状态,
    case b.pay_status
        when 1 then '无需支付'
        when 2 then '未支付'
        when 3 then '已支付'
        end 支付状态,
    case b.write_off_status
        when 1 then '未核销'
        when 2 then '部分核销'
        when 3 then '已核销'
        end 核销状态,
    l.po_order_code as 采购单号,
    -- cast(l.apply_amount as double) as 申请金额,
    cast(b.apply_total_amount as double) as 本次申请总金额,
    -- sp.supplier_name as 供应商,
    b.bill_time as 付款申请日期,
    b.submit_to_pay_time as 发起付款时间
FROM ods.ods_lh_dss_ss_payment_apply_bill_df AS b
     INNER JOIN ods.ods_lh_dss_ss_payment_apply_bill_line_df AS l
        ON b.bill_code = l.payment_apply_bill_code
        AND l.record_status = 1
     join (select org_code, org_name, invoice_flag from ods.ods_itop_fs_sh_organization_df where purchase_org_flag = 1) as org
        on org.org_code = b.purchase_subject_code
join ods.ods_lh_dsm_ps_supplier_df as sp on sp.id = b.ps_supplier_id
where org.org_name ='常熟市莫城街道浩兴通百货商行（个体工商户）'
    and b.record_status = 1
    and sp.supplier_name = '徐州世昌玻璃制品有限公司'
order by bill_time, bill_code, po_order_code
;
