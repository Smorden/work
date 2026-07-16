select bs.sku, sp.supplier_name as 供应商, cast(bs.num_in_box as int) 装箱数
             , ifnull(cast(pr.default_supplier_flag as int), 0) 是否默认供应商
from ods.ods_lh_dsm_ps_supplier_box_specification_df as bs
     join ods.ods_lh_dsm_ps_supplier_df as sp on sp.id = bs.ps_supplier_id and sp.record_status = 1
     left join ods.ods_lh_dsm_ps_sku_pricing_df as pr on pr.sku = bs.sku and pr.ps_supplier_id = bs.ps_supplier_id and pr.record_status = 1
where bs.record_status = 1
  and default_spec_flag = 1
order by bs.sku, sp.supplier_name
;