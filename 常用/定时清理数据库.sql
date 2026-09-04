select table_name, data_length/1024/1024/1024
from information_schema.`tables`
order by data_length desc
limit 20
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
truncate table ods.ods_pbbs_sj_jdy_ebay_abnormal_follow_new_di partitions(

)
;