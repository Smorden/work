with base as (
select paid_time, order_id, order_lineitemid, sku, parcel_id, sku_qty, parcel_sku_qty
from dwm.dwm_tcct_sales_order_fulfillment_sku_details_di
where date(paid_time) between '2025-07-01' and '2026-04-30'
and account_id = 'PANGYUN'
and parcel_status not in ('','已取消')
)
, ivct_batch as (
    select distinct sf.order_num_origin as parcel_id
        ,sf.sku, sf.parent_batch_code, sf.child_batch_code
    from dwd.dwd_fact_ivct_batch_stock_flowing_di as sf
    join (select distinct parcel_id from base) as pc on pc.parcel_id = sf.order_num_origin
    where sf.order_type_id = 3
    and sf.qty < 0
    and sf.order_status = 3
    and sf.record_status = 1
    and sf.child_batch_code <> ''
)
, fnct_batch as (
    select distinct sf.origin_num as parcel_id
        ,sf.sku
        , ba.init_batch_code as parent_batch_code
        , ba.init_child_batch_code as child_batch_code
    from dwd.dwd_fact_fnct_cost_order_line_batch_di as sf
    join (select distinct parcel_id from base) as pc on pc.parcel_id = sf.origin_num
    left join dws.dws_fnct_batch_sku_version_df as ba on ba.batch_code = sf.batch_code
    where sf.origin_type_code = 'OMS_FH'
    and sf.fix_in_out_director = -1
)
, container_no as (
select ba.*
    , ifnull(bm.move_in_order_no, bm2.move_in_order_no) as move_in_order_no
    , sn.cabinet_no
    , df.container_no
from(
    select *
    from ivct_batch
    union all
    select *
    from fnct_batch
) as ba
left join dws.dws_ivct_transfer_in_to_oversea_in_batch_merge_df as bm on bm.oversea_in_child_batch_code = ba.child_batch_code
left join (
    select batch_code, min(move_in_order_no) as move_in_order_no
    from dws.dws_ivct_transfer_in_to_oversea_in_batch_merge_df
    where move_in_order_no <> ''
    group by batch_code
    ) as bm2 on bm2.batch_code = ba.parent_batch_code
    and bm.oversea_in_child_batch_code is null
left join dws.dws_ivct_first_trip_flow_ship_no_df as sn on sn.ship_no = ifnull(bm.move_in_order_no, bm2.move_in_order_no)
left join dwd.dwd_fact_trct_logistics_first_trip_documentary_flow_df as df on df.cabinet_no = sn.cabinet_no
)
, purchase_sub_name as (
    select sn.ship_no, og.org_name as purchase_subject_name
    from(
    select ship_no, purchase_subject_code
    from dwd.dwd_dim_ivct_ship_info_ds
    where dt = date_sub(curdate(), interval 1 day)
    ) as sn
    join ods.ods_itop_fs_sh_organization_df as og on og.org_code = sn.purchase_subject_code
)
select
    distinct bs.paid_time 付款时间, bs.order_id, bs.parcel_id 包裹单号, bs.sku
    , cn.move_in_order_no 调拨单号, ps.purchase_subject_name 采购主体, cn.container_no 集装箱号
from base as bs
left join container_no as cn on cn.parcel_id = bs.parcel_id and cn.sku = bs.sku
left join purchase_sub_name as ps on ps.ship_no = cn.move_in_order_no
where cn.move_in_order_no <> ''
order by bs.paid_time, bs.order_id, bs.parcel_id, bs.sku
;