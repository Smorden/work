with dim_goods as (
	select sku, goods_code
	from dwd.dwd_dim_goods_ds
	where dt = date_sub(curdate(), interval 1 day)
)
,dim_supplier as (
	select supplier_id, province_name, city_name
	from dwd.dwd_dim_supplier_ds
	where dt = date_sub(curdate(), interval 1 day)
)
,supplier_delivery_price as (
	select sku, ps_supplier_id, theoretical_purchase_price, factory_theoretical_purchase_price
		, theoretical_purchase_price - factory_theoretical_purchase_price as supplier_delivery_price
	from dwd.dwd_dim_supplier_sku_ds
	where dt = date_sub(curdate(), interval 1 day)
	and factory_theoretical_purchase_price > 0
	and theoretical_purchase_price - factory_theoretical_purchase_price > 0
)
select
	ts.dt 收货日期
	, pb.po_order_code 采购单, gd.sku
	, max(po.ps_supplier_name) as 下单供应商
	, max(po.deliver_supplier_name) as 发货供应商
	, left(max(sp.province_name), 2) as 省份
	, replace(max(sp.city_name), '市', '') as 城市
	, count(ts.before_wms_carton_num) as 箱数
	, sum(ts.before_qty) as si数量
	, sum(ts.box_volume )/1000000.0 as 体积m3
	, sum(ts.box_weight) as 重量kg
	, sum(ts.before_qty * po.purchase_price) as  含运采购金额
	, max(if(dp.sku is null, '是', '否')) as 是否异常
	, sum(ifnull(dp.supplier_delivery_price, 0) * ts.before_qty) as 自发运费
	, max(if(po.order_label = 1, '是', '否')) as 是否异地整柜
from (
	select *
	from dwd.dwd_fact_mk_wms_wms_transaction_all_di
	where transaction_type  = '收货'
		and insert_flag = 0
		and dt between '2026-01-01' and date_sub(curdate(), interval 1 day)
	) as ts
join dwd.dwd_dim_mk_wms_wmsin_parent_batch_di as pb on pb.parent_batch_code = ts.before_batch_no and pb.record_status = 1
join dim_goods as gd on gd.goods_code = ts.before_goods_code
join dwd.dwd_fact_pcct_purchase_order_line_df as po on po.po_order_code = pb.po_order_code
	and po.sku = gd.sku
join dim_supplier as sp on sp.supplier_id = if(po.deliver_supplier_id<> 0, po.deliver_supplier_id, po.ps_supplier_id)
left join supplier_delivery_price as dp on dp.sku = gd.sku and dp.ps_supplier_id = po.ps_supplier_id
where sp.province_name <> ''
group by ts.dt, pb.po_order_code, gd.sku
order by ts.dt, pb.po_order_code, gd.sku
;