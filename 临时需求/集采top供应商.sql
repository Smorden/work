with po_amt as (
    select
        ps_supplier_name, ps_supplier_id, bom_sku
                        , sum(line_total_amount_with_tax) as po_amt
    from dwd.dwd_fact_pcct_purchase_order_line_df
    where
          po_order_date >= date_sub(curdate(), interval 3 month)
      and not (po_order_line_status in (30, 70) and shelved_qty = 0)
      and pickup_method = 1
    -- and order_label <> 2
    group by
        ps_supplier_name, ps_supplier_id, bom_sku
    )
select t.供应商, t.付款渠道, t.采购额, t.排名, sku.bom_sku as sku, sku.po_amt as sku采购额
from
    (
        select
            po.ps_supplier_name                            供应商
          , pay_channel                                    付款渠道
          , po.po_amt                                      采购额
          , row_number() over (order by po.po_amt desc) as 排名
            , po.ps_supplier_id
        from
            (
                select ps_supplier_name, ps_supplier_id, sum(po_amt) as po_amt
                from po_amt
                group by ps_supplier_name, ps_supplier_id
                )                                                as po
            join (
                select
                    supplier_id
                  , pay_channel
                from dwd.dwd_dim_supplier_ds
                where
                      dt = date_sub(curdate(), interval 1 day)
                  and pay_channel not in ('跨境宝', 'kuajing')
                )                                                as ps
                on ps.supplier_id = po.ps_supplier_id
            join ods.ods_lh_dpe_pe_org_purchase_rule_supplier_df as pr
                on pr.record_status = 1 and pr.rule_code = 'JC00100516' and pr.ps_supplier_id = po.ps_supplier_id
        ) as t
join po_amt as sku on sku.ps_supplier_id = t.ps_supplier_id
where 排名 <= 20
order by t.排名, sku.po_amt desc
;
select distinct pay_channel
from dwd.dwd_dim_supplier_ds
where dt = date_sub(curdate(), interval 1 day)
;
select a.sku, a.supplier_name, d.org_name
from
    (
        select sku, ps_supplier_id, supplier_name, default_supplier_flag from dwd.dwd_dim_supplier_sku_ds where dt = '2026-09-15' and supplier_rank = 1
        ) as a
left join ods.ods_lh_dpe_pe_org_purchase_rule_supplier_df as b on b.ps_supplier_id = a.ps_supplier_id and b.record_status = 1
left join ods.ods_lh_dpe_pe_org_purchase_rule_df as c on c.rule_code = b.rule_code and c.record_status = 1
left join ods.ods_itop_fs_sh_organization_df as d on d.purchase_org_flag = 1 and d.org_code = c.purchase_subject_code
where a.sku in (
                '4XZHONGXMJJJX','AH253902001','AH254001001','AH261307001','AJ2412301001','AJ246202002','AJ246501001','AJ246501002','AJ246501003','AJ2521303001','AJ2521303002','AJ2521303003','AJ2523301001','AJ2523301002','AJ2523301003','AJ2534601001','AJ2534803001','AJ2534903001','AJ2536605001','AJ2536705001','AJ2536705002','AJ2537903001','AJ2537903002','AJ2539205001','AJ2539205002','AJ2539401003','AJ2539401004','AJ2539401005','AJ2539401006','AJ2539601001','AJ2539601002','AJ2539601003','AJ2539601004','AJ2541003001','AJ2543002001','AJ2545101001','AJ2545101002','AJ2546301001','AJ2546301002','AJ2547203001','AJ2547203002','AJ259704001','AJ2613755001','AJ2615391001','AJ262808001','AJ262808002','AJ262808003','AJ2631056001','AJ2632097001','AJ265141001','AJ265141002','AJ265368002','AJ267992001','AJ267992002','AJ267992003','AU257501002','AU2614388001','AU2614388002','AU2614388003','AU2614388004','AU2614388005','AU2614388006','AU2618746001','AU2618746002','AU2618746003','AU2618746004','AU2618746005','AU2618746006','AU263934001','AW2414307001','AW2414307002','AW2414307003','AW2414307004','AW2415601001','AW2423201001','AW2514102001','AW2514102002','AW2514102003','AW2518616001','AW2521101001','AW2523804002','AW2523804003','AW2528501001','AW253101001','AW253101002','AW2533204001','AW2533204002','AW2533206001','AW2533206002','AW253705001','AW2537901001','AW2537901002','AW2537901003','AW2537901004','AW2537903001','AW2537903002','AW2537903003','AW2537903004','AW2537904002','AW2542811001','AW2542811002','AW2550004001','AW258013001','AW258013002','AW258319001','AW2611209003','AW2614393001','AW2619798001','AW2625655001','AW263704001','AW263704002','AW263704003','AW263704004','AW2637271001','AW2637271002','AW2637272001','AW265227001','AW266011001','AW266334001','AW266334002','AW266539001','AW268339001','AY255602001','AY2624747001','AY2624747002','AZ1168555A1','AZ251001001','BB253302003','BB253302004','BB254101003','BB254101004','BB256301001','BG261001001','CE263110001','CE264534001','CI264242001','CI264242002','CI264242003','CI264242004','CL254801001','CL254802001','CL256001001','CL256002001','CL257601001','CP260401001','CQ256801001','CQ261601001','CQ263401001','CQ263401002','CQ263401003','CQ263401004','CQ263401005','CQ263401006','CQ263401007','CQ263401008','CQ263401009','CQ263401010','CR2511401001','CR251504001','CR2515202001','CR2518811001','CR2519003001','CR2521001001','CR2522701001','CR2523901001','CR2524003001','CR2524102001','CR2524903001','CR2525009001','CR2525111001','CR2525111002','CR2525301001','CR2525301002','CR2525505001','CR2526201001','CR2527002001','CR2527805001','CR2527805002','CR2527805003','CR2528906001','CR2529817001','CR2530503001','CR2530503002','CR2530503003','CR2530503004','CR2530507001','CR2530507002','CR2530507003','CR2530507004','CR2530720001','CR2531111004','CR2531111005','CR2531111006','CR2531503001','CR261608001','CR261714001','CR261804001','CR261804002','CR2625279001','CR2625640001','CR262704001','CR262827001','CR2631777001','CR263203001','CR265077001','CR265077002','CR265202001','CR265202002','CR265202003','CR265202004','CR265207001','CR265440001','CR267776001','CR268694001','CR269970001','CR269972001','CR269972002','CS260001001','CS260401001','CT2511802001','CT2613381001','CT267707001','CV264815001','CV264815002','CV266443001','EZ1161042A1','EZ1161369A1','EZ1163524A1','EZ1165040A1','EZ1167126A1','EZ1172584A1','EZ1172585A1','EZ1177518A1','EZ1182965A1','EZ1184287A1','MAOGYSQ','MODSTZ','OT+MDZSJQ','OT+SJQ+80MM','OT+SLP+30P','OT+ZPG+DE'
    )
order by a.sku
;
select sku_type
from dwd.dwd_dim_sku_ds
where dt = date_sub(curdate(), interval 1 day)
and sku = 'CR2631777001'
;