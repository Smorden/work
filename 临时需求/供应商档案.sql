select distinct pay_channel
    from ods.ods_lh_dsm_ps_supplier_df
where record_status = 1
;
select bs.supplier_code as 供应商编码
     ,bs.supplier_name as 供应商名称
     ,bs.pay_channel as 付款渠道
     ,pm.pay_method_alias 付款方式
     ,bs.purchase_responser as  采购负责人
from
    (
        select * from dwd.dwd_dim_supplier_ds
                 where dt = date_sub(curdate(), interval 1 day)
                 and supplier_biz_status = 4
                 and pay_channel <> '跨境宝'
                 and supplier_type = 1
        ) as bs
join ods.ods_lh_dsm_ps_supplier_df as b on bs.supplier_id = b.id
join dwd.dwd_dim_pcct_pay_method_df as pm on bs.pay_method_code = pm.pay_code
and b.record_status = 1 and b.delivery_method = 1
order by bs.supplier_code
;
select pay_channel from dwd.dwd_dim_supplier_ds
where dt = date_sub(curdate(), interval 1 day)
  and supplier_biz_status = 4

;