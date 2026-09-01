select po.ps_supplier_name 供应商, pay_channel 付款渠道, po.po_amt 采购额
                                 , row_number() over (order by po_amt desc) as 排名
from
    (
        select
            ps_supplier_name, ps_supplier_id
                            , sum(line_total_amount_with_tax) as po_amt
        from dwd.dwd_fact_pcct_purchase_order_line_df
        where
              po_order_date >= date_sub(curdate(), interval 3 month)
          and not (po_order_line_status in (30, 70) and shelved_qty = 0)
          and pickup_method = 1
        -- and order_label <> 2
        group by
            ps_supplier_name, ps_supplier_id
        ) as po
    join (
        select
            supplier_id, pay_channel
        from dwd.dwd_dim_supplier_ds
        where
              dt = date_sub(curdate(), interval 1 day)
          and pay_channel not in ('跨境宝', 'kuajing')
        ) as ps on ps.supplier_id = po.ps_supplier_id
    join ods.ods_lh_dpe_pe_org_purchase_rule_supplier_df as pr on pr.record_status = 1
        and pr.rule_code = 'JC00100516' and pr.ps_supplier_id = po.ps_supplier_id
order by po.po_amt desc
limit 20
;
select distinct pay_channel
from dwd.dwd_dim_supplier_ds
where dt = date_sub(curdate(), interval 1 day)
;