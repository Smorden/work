select table_schema, table_name, data_length/1024/1024/1024 data_space
from information_schema.`tables`
where data_length/1024/1024/1024 >= 20
order by data_length/1024/1024/1024 desc 
;

;
select min(dt) from dws.dws_ivct_stock_health_assessment_di;
select min(dt) from dws.dws_ivct_stock_health_assessment_country_ds;
select min(dt) from dws.dws_opct_forecast_sales_sum_ds;
select min(dt) from ads.ads_sad_future_oversea_stock_turnover_ds;

select dt, count(1)
from ods.ods_pbbs_sj_jdy_ebay_abnormal_follow_new_di
group by dt
order by dt
;
truncate table dws.dws_ivct_stock_health_assessment_di partitions(
p_20250101,
p_20250102,
...
p_20251231
)
;
生产环境需要把历史数据进行备份转移，我的初步想法是筛选出10g（可调）以上的大表（或者占用空间前80%的大表），查询后续使用的范围和本表的数据更新情况，确认本表的备份范围，涉及看板利润的表最好不动，我列出的这13张表也可以不动，我会手动清理
select table_schema, table_name, data_length/1024/1024/1024 data_space
from information_schema.`tables`
where data_length/1024/1024/1024 >= 10
order by data_length/1024/1024/1024 desc 
;
dws.dws_ivct_stock_health_assessment_di
dws.dws_ivct_stock_health_assessment_country_ds
dws.dws_opct_forecast_sales_sum_ds
ads.ads_sad_future_oversea_stock_turnover_ds
ads.ads_scad_stock_calendar_ds
dws.dws_ivct_ship_country_future_oversea_stock_turnover_ds
dws.dws_opct_forecast_sales_sum_ship_country_ds
dws.dws_ivct_sku_ship_scope_current_supply_days_base_ds
dws.dws_ivct_sku_ship_country_future_oversea_stock_ds
dws.dws_ivct_site_future_oversea_stock_turnover_ds
dws.dws_ivct_sku_site_future_oversea_stock_ds
dws.dws_ivct_sku_site_current_supply_days_base_ds
dws.dws_ivct_sku_dc_current_supply_days_ds

查询以上几张表的最小dt，并按如下格式给出truncate执行脚本。
truncate table dws.dws_ivct_stock_health_assessment_di partitions(
p_20250101(如果最小日期<20250101,则从最小日期开始删),
p_20250102,
...
p_20251231
);

彻底删除不走回收站，drop table force;

那13张表，我要继续删分区，根据数据引用情况给出可删分区的范围，再给我上次一样的truncate partition的脚本


- ods_pbbs_oc_so_order_di 不要动
- 3 张 ads_finebi_* 不要动。
- ods_odoo_erp_stock_quant_move_rel_di调度已停，这个可以在元数据库sheduler_meta.table_info里面的is_enable字段进行判断，或者config里面的exec为:echo 1也行，odoo和oms有很多类似表，可以加进来，但是容量小的df表不要动，可能是维度表。


dwd_dim_goods_ds 保留26年分区
ads.ads_finebi_listing_check_ds truncate 保留7天
ads.ads_finebi_tiktok_upload_inventory_result_ds truncate 保留7天

ads_finebi_listing_check_ds,ads_finebi_tiktok_upload_inventory_result_ds这两张的truncate sql给我

ods_pbbs_oc_和ods_odoo_表备份，其他ods表直接truncate 26年之前的分区，不做备份

dwm_tcct_oms_orders_detail_di这张表保留，不做处理
dws_mk_spring_expected_future_sales_channel_df 直接truncate
dws_mk_spring_daily_rp_sales_df 直接truncate

truncate table dws.dws_mk_spring_expected_future_sales_channel_df;
truncate table dws.dws_mk_spring_daily_rp_sales_df;

drop table dws.dws_mk_spring_expected_future_sales_channel_df;
drop table dws.dws_mk_spring_daily_rp_sales_df;

ads.ads_finebi_sku_site_lack_net_profit_ds 已清理
ads.ads_finebi_tiktok_upload_inventory_result_ds 只留近7天分区，其他truncate
ads.ads_finebi_listing_check_ds 只留近7天分区，其他truncate