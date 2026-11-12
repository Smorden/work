with dim_goods as (
    select sku, goods_code
    from dwd.dwd_dim_goods_ds
    where dt = date_sub(curdate(), interval 1 day)
    )
  , dim_supplier as (
    select supplier_id, province_name, city_name
    from dwd.dwd_dim_supplier_ds
    where dt = date_sub(curdate(), interval 1 day)
    )
  , supplier_delivery_price as (
    select sku, ps_supplier_id, theoretical_purchase_price, factory_theoretical_purchase_price
              , theoretical_purchase_price - factory_theoretical_purchase_price as supplier_delivery_price
    from dwd.dwd_dim_supplier_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
      and factory_theoretical_purchase_price > 0
      and theoretical_purchase_price - factory_theoretical_purchase_price > 0
    )
  , base as (
    select ts.dt as receive_date
      , pb.po_order_code, gd.sku
      , max(po.ps_supplier_name) as ps_supplier_name
      , max(po.deliver_supplier_name) as deliver_supplier_name
      , left(max(sp.province_name), 2) as province
      , replace(max(sp.city_name), '市', '') as city
      , count(ts.before_wms_carton_num) as carton_num
      , sum(ts.before_qty) as si_qty
      , sum(ts.box_volume) / 1000000.0 as volume_m3
      , sum(ts.box_weight) as weight_kg
      , max(if(dp.sku is null, '是', '否')) as is_abnormal
      , sum(ifnull(dp.supplier_delivery_price, 0) * ts.before_qty) as self_freight
      , max(if(po.order_label = 1, '是', '否')) as is_remote_fcl
      , max(po.purchase_subject_name) as purchase_org
    from (
             select *
             from dwd.dwd_fact_mk_wms_wms_transaction_all_di
             where transaction_type = '收货'
               and insert_flag = 0
               and dt between '2026-01-01' and date_sub(curdate(), interval 1 day)
             ) as ts
         join dwd.dwd_dim_mk_wms_wmsin_parent_batch_di as pb on pb.parent_batch_code = ts.before_batch_no and pb.record_status = 1
         join dim_goods as gd on gd.goods_code = ts.before_goods_code
         join dwd.dwd_fact_pcct_purchase_order_line_df as po on po.po_order_code = pb.po_order_code
            and po.sku = gd.sku
         join dim_supplier as sp on sp.supplier_id = if(po.deliver_supplier_id <> 0, po.deliver_supplier_id, po.ps_supplier_id)
         left join supplier_delivery_price as dp on dp.sku = gd.sku and dp.ps_supplier_id = po.ps_supplier_id
    where sp.province_name <> ''
    group by ts.dt, pb.po_order_code, gd.sku
    )
  , base_k as (
    select b.*
      , substring(b.province, 1, 2) as prov2
      , if(substring(b.city, 1, 2) = '襄樊', '襄阳', substring(b.city, 1, 2)) as city2
      , cast(b.weight_kg as decimal(27,9)) as k_weight
      , cast(b.volume_m3 as decimal(27,9)) as k_volume
      , if(b.purchase_org like '%汇岚途%', '汇岚途', '') as k_org
    from base as b
    )
  , inp as (
    select row_number() over(order by prov2, city2, weight, volume, org) as idx
      , prov2, city2, weight, volume, org
      , greatest(volume / 0.006, weight) as w1
    from (
             select distinct prov2, city2, k_weight as weight, k_volume as volume, k_org as org
             from base_k
             where ifnull(prov2, '') <> '' and ifnull(city2, '') <> '' and k_weight >= 0 and k_volume >= 0
             ) as t
    )
  , sf as (
    select i.idx, min(if((cast(nullif(trim(q.first_20kg_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 20, 0, (floor(i.w1 + 0.5) - 20) * cast(nullif(trim(q.extra_weight_fee_1),'') as decimal(18,4)))) < (cast(nullif(trim(q.first_60kg_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 60, 0, (floor(i.w1 + 0.5) - 60) * cast(nullif(trim(q.extra_weight_fee_2),'') as decimal(18,4)))), (cast(nullif(trim(q.first_20kg_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 20, 0, (floor(i.w1 + 0.5) - 20) * cast(nullif(trim(q.extra_weight_fee_1),'') as decimal(18,4)))), (cast(nullif(trim(q.first_60kg_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 60, 0, (floor(i.w1 + 0.5) - 60) * cast(nullif(trim(q.extra_weight_fee_2),'') as decimal(18,4))))) * 0.6) as fee
    from inp as i
         join ods.ods_md_me_offline_lgct_pickup_quote_sf_kahang_df as q
            on substring(q.dest_province, 1, 2) = i.prov2 and instr(q.dest_city, i.city2) > 0
    where q.create_date = (select max(create_date) from ods.ods_md_me_offline_lgct_pickup_quote_sf_kahang_df)
    group by i.idx
    )
  , dbkd as (
    select i.idx, min(if((cast(nullif(trim(q.std_first_weight_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 1, 0, (floor(i.w1 + 0.5) - 1) * cast(nullif(trim(q.std_extra_weight_fee),'') as decimal(18,4)))) < (cast(nullif(trim(q.bulky_first_weight_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 3, 0, (floor(i.w1 + 0.5) - 3) * cast(nullif(trim(q.bulky_extra_weight_fee),'') as decimal(18,4)))), (cast(nullif(trim(q.std_first_weight_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 1, 0, (floor(i.w1 + 0.5) - 1) * cast(nullif(trim(q.std_extra_weight_fee),'') as decimal(18,4)))), (cast(nullif(trim(q.bulky_first_weight_fee),'') as decimal(18,4)) + if(floor(i.w1 + 0.5) < 3, 0, (floor(i.w1 + 0.5) - 3) * cast(nullif(trim(q.bulky_extra_weight_fee),'') as decimal(18,4)))))) as fee
    from inp as i
         join ods.ods_md_me_offline_lgct_pickup_quote_deppon_express_df as q
            on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2
    group by i.idx
    )
  , dbky_q as (
    select *, row_number() over(partition by substring(origin_province, 1, 2), substring(origin_city, 1, 2) order by quote_date desc) as rn
    from ods.ods_md_me_offline_lgct_pickup_quote_deppon_freight_df
    where quote_date <= cast(curdate() as varchar)
    )
  , dbky as (
    select i.idx, min(cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + if(i.w1 < cast(nullif(trim(q.first_weight_kg),'') as decimal(18,4)), 0, (i.w1 - cast(nullif(trim(q.first_weight_kg),'') as decimal(18,4))) * cast(nullif(trim(q.extra_weight_unit_price),'') as decimal(18,4)))) as fee
    from inp as i
         join dbky_q as q
            on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2 and q.rn = 1
    group by i.idx
    )
  , kytc as (
    select i.idx, greatest(cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + case
                                                                                            when i.w1 <= 1 then 0
                                                                                            when i.w1 <= 50 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_1_50kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 100 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_50_100kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 300 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_100_300kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 500 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_300_500kg),'') as decimal(18,4))
                                                                                            else (i.w1 - 1) * cast(nullif(trim(q.unit_price_500_plus_kg),'') as decimal(18,4)) end
        , ifnull(cast(nullif(trim(q.min_freight),'') as decimal(18,4)), -1e15)) as fee
    from inp as i
         join ods.ods_md_me_offline_lgct_pickup_quote_kye_intracity_df as q
            on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2
    )
  , kylj_q as (
    select *, row_number() over(partition by substring(origin_province, 1, 2), substring(origin_city, 1, 2) order by quote_date desc) as rn
    from ods.ods_md_me_offline_lgct_pickup_quote_kye_land_df
    where quote_date <= cast(curdate() as varchar)
    )
  , kylj as (
    select i.idx, greatest(cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + case
                                                                                            when i.w1 <= 1 then 0
                                                                                            when i.w1 <= 50 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_1_50kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 100 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_50_100kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 200 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_100_200kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 300 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_200_300kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 500 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_300_500kg),'') as decimal(18,4))
                                                                                            when i.w1 <= 1000 then (i.w1 - 1) * cast(nullif(trim(q.unit_price_500_1000kg),'') as decimal(18,4))
                                                                                            else (i.w1 - 1) * cast(nullif(trim(q.unit_price_1000_plus_kg),'') as decimal(18,4)) end
        , ifnull(cast(nullif(trim(q.min_freight),'') as decimal(18,4)), -1e15)) as fee
    from inp as i
         join kylj_q as q
            on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2 and q.rn = 1
    )
  , ky as (
    select idx, if(min(fee) <= 34, 34, min(fee)) + 2 as fee
    from (select idx, fee from kytc union all select idx, fee from kylj) as t
    group by idx
    )
  , ej_q as (
    select q.*, i.idx, i.weight, i.volume
    from inp as i
         join ods.ods_md_me_offline_lgct_pickup_quote_enjia_df as q
            on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2
    )
  , ejzh_q as (
    select * from ods.ods_md_me_offline_lgct_pickup_quote_enjie_heavy_df
    where substring(create_time, 1, 10) = (select max(substring(create_time, 1, 10)) from ods.ods_md_me_offline_lgct_pickup_quote_enjie_heavy_df)
    )
  , ej as (
    select idx, min(fee) as fee
    from (
             select idx, cast(nullif(trim(ftl_13m_fee),'') as decimal(18,4)) as fee from ej_q where weight <= 30000 and volume <= 70
             union all
             select idx, cast(nullif(trim(ftl_17_5m_fee),'') as decimal(18,4)) as fee from ej_q where weight <= 30000 and volume <= 110
             union all
             select idx, if(weight * cast(nullif(trim(weight_unit_price),'') as decimal(18,4)) > volume * cast(nullif(trim(volume_unit_price),'') as decimal(18,4)), weight * cast(nullif(trim(weight_unit_price),'') as decimal(18,4)), volume * cast(nullif(trim(volume_unit_price),'') as decimal(18,4)))
                 + if(weight >= 5000 or volume >= 15, 0, cast(nullif(trim(pickup_fee),'') as decimal(18,4)) + cast(nullif(trim(delivery_fee),'') as decimal(18,4))) as fee
             from ej_q
             union all
             select i.idx, if(if(i.weight <= 18000, cast(nullif(trim(q.unit_price_10000_18000kg),'') as decimal(18,4)), cast(nullif(trim(q.unit_price_18000_plus_kg),'') as decimal(18,4))) * i.weight >= i.volume * cast(nullif(trim(q.volume_unit_price),'') as decimal(18,4))
                 , if(i.weight <= 18000, cast(nullif(trim(q.unit_price_10000_18000kg),'') as decimal(18,4)), cast(nullif(trim(q.unit_price_18000_plus_kg),'') as decimal(18,4))) * i.weight, i.volume * cast(nullif(trim(q.volume_unit_price),'') as decimal(18,4))) as fee
             from inp as i
                  join ejzh_q as q on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2
             where i.weight > 10000
             ) as t
    group by idx
    )
  , jd as (
    select i.idx, min(case
                          when i.w1 <= 20 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4))
                          when i.w1 <= 60 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_1),'') as decimal(18,4))
                          else cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + 40 * cast(nullif(trim(q.extra_weight_fee_1),'') as decimal(18,4)) + (i.w1 - 60) * cast(nullif(trim(q.extra_weight_fee_2),'') as decimal(18,4)) end * 0.55 + 12) as fee
    from inp as i
         join ods.ods_md_me_offline_lgct_pickup_quote_jd_express_df as q
            on substring(q.origin_province, 1, 2) = i.prov2 and substring(q.origin_city, 1, 2) = i.city2
    group by i.idx
    )
  , bsky as (
    select i.idx, min(greatest(case
                                   when i.w1 <= 20 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4))
                                   when i.w1 <= 70 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_1),'') as decimal(18,4))
                                   when i.w1 <= 1000 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_2),'') as decimal(18,4))
                                   when i.w1 <= 3000 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_3),'') as decimal(18,4))
                                   when i.w1 <= 5000 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_4),'') as decimal(18,4))
                                   when i.w1 <= 10000 then cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_5),'') as decimal(18,4))
                                   else cast(nullif(trim(q.first_weight_fee),'') as decimal(18,4)) + (i.w1 - 20) * cast(nullif(trim(q.extra_weight_fee_6),'') as decimal(18,4)) end
        , ifnull(cast(nullif(trim(q.min_charge),'') as decimal(18,4)), -1e15))) as fee
    from inp as i
         join ods.ods_md_me_offline_lgct_pickup_quote_best_freight_df as q on substring(q.origin_province, 1, 2) = i.prov2
    group by i.idx
    )
  , bstc as (
    select i.idx, min(case
                          when i.w1 < 800 then 9999996
                          when i.w1 < 1500 then i.w1 * cast(nullif(trim(q.unit_price_800_1500kg),'') as decimal(18,4))
                          when i.w1 < 3000 then i.w1 * cast(nullif(trim(q.unit_price_1500_3000kg),'') as decimal(18,4))
                          else i.w1 * cast(nullif(trim(q.unit_price_3000_plus_kg),'') as decimal(18,4)) end + 3) as fee
    from inp as i
         left join ods.ods_md_me_offline_lgct_pickup_quote_best_fill_df as q on substring(q.origin_province, 1, 2) = i.prov2
    group by i.idx
    )
  , res as (
    select i.idx, i.org, i.prov2, i.city2, i.weight, i.volume
                , dbkd.fee as deppon_express, dbky.fee as deppon_freight, jd.fee as jd, ky.fee as kye
                , bsky.fee as best_freight, ej.fee as enjia, bstc.fee as best_fill, sf.fee as sf
    from inp as i
         left join dbkd on dbkd.idx = i.idx
         left join dbky on dbky.idx = i.idx
         left join jd on jd.idx = i.idx
         left join ky on ky.idx = i.idx
         left join bsky on bsky.idx = i.idx
         left join ej on ej.idx = i.idx
         left join bstc on bstc.idx = i.idx
         left join sf on sf.idx = i.idx
    )
  , best as (
    select *, nullif(least(ifnull(deppon_express, 1e15), ifnull(deppon_freight, 1e15), ifnull(jd, 1e15), ifnull(kye, 1e15)
                         , ifnull(best_freight, 1e15), ifnull(best_fill, 1e15), ifnull(enjia, 1e15), ifnull(sf, 1e15)), 1e15) as best_fee
    from res
    )

select k.receive_date as 收货日期
  , k.po_order_code as 采购单
  , k.sku
  , k.ps_supplier_name as 下单供应商
  , k.deliver_supplier_name as 发货供应商
  , k.province as 省份
  , k.city as 城市
  , k.carton_num as 箱数
  , k.si_qty as si数量
  , k.volume_m3 as 体积m3
  , k.weight_kg as 重量kg
  , k.is_abnormal as 是否异常
  , k.self_freight as 自发运费
  , k.is_remote_fcl as 是否异地整柜
  , case
        when b.best_fee is null then ''
        when b.best_fee = b.deppon_express then '德邦快递'
        when b.best_fee = b.deppon_freight then '德邦快运'
        when b.best_fee = b.jd then '京东'
        when b.best_fee = b.kye then '跨越'
        when b.best_fee = b.best_freight then '百世快运'
        when b.best_fee = b.best_fill then '百世填仓'
        when b.best_fee = b.enjia and b.org = '汇岚途' then '恩佳供应链'
        when b.best_fee = b.enjia then '恩捷供应链'
        when b.best_fee = b.sf then '顺丰'
        end as 下单物流商
  , round(b.best_fee, 2) as 最优自提运费
  , round(b.deppon_express, 2) as 德邦快递
  , round(b.deppon_freight, 2) as 德邦快运
  , round(b.jd, 2) as 京东物流
  , round(b.kye, 2) as 跨越
  , round(b.best_freight, 2) as 百世快运
  , round(b.enjia, 2) as 恩佳供应链
  , round(b.best_fill, 2) as 百世填仓
  , round(b.sf, 2) as 顺丰
  , if(k.is_abnormal = '否', k.self_freight - ifnull(round(b.best_fee, 2), 0), 0) as 自发自提运费差
from base_k as k
     left join best as b on b.prov2 <=> k.prov2 and b.city2 <=> k.city2 and b.weight <=> k.k_weight and b.volume <=> k.k_volume and b.org <=> k.k_org
order by k.receive_date, k.po_order_code, k.sku
;