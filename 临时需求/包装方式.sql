with sku_pack as (
    SELECT sku,aod.attribute_val as pack_type
        , row_number() over (partition by sku order by aod.id desc) as rn
        FROM dwd.dwd_dim_sku_ds ddsd
    inner join (SELECT * FROM ods.ods_lh_dpc_pc_product_attr_df WHERE data_type = 'SPU'  ) ad
    on ddsd.spu_id =ad.data_id
     left JOIN ods.ods_lh_dpc_pc_attribute_df                                      atd
                            ON ad.code = atd.code
                            and atd.record_status = 1
    left JOIN ods.ods_lh_dpc_pc_product_attr_optional_df                          aod
    ON ad.id = aod.product_attr_id
        AND aod.attribute_val_status = 1
        where  name='包装方式'
        and ddsd.dt =date_sub(curdate(), interval 1 day)
        and lv1_category_name <> '自用耗材'
)
,dim_goods as (
	select sku, goods_code
	from dwd.dwd_dim_goods_ds
	where dt = date_sub(curdate(), interval 1 day)
)
select ifnull(sp.pack_type, '无') as 包装方式
    , count(distinct ts.before_batch_no) as asn单数
    , count(ts.before_wms_carton_num) as 箱数
	, sum(ts.before_qty) as si数量
	, sum(ts.box_volume )/1000000.0 as 体积m3
	, sum(ts.box_weight) as 重量kg
from (
	select *
	from dwd.dwd_fact_mk_wms_wms_transaction_all_di
	where transaction_type  = '收货'
		and insert_flag = 0
		and dt between '2026-01-01' and date_sub(curdate(), interval 1 day)
	) as ts
	join dim_goods as gd on gd.goods_code = ts.before_goods_code
	join sku_pack as sp on sp.sku = gd.sku and sp.rn = 1
	group by ifnull(sp.pack_type, '无')
	order by ifnull(sp.pack_type, '无')
    ;