with smooth_sales as(
	select sku, platform_id, site_id
		, round(sum(clean_avg_sales_7d), 2) as avg_sales_7d
		, round(sum(clean_qty_smooth_short), 2) as smooth_sales_short
		, round(sum(clean_qty_smooth), 2) as smooth_sales
	from dws.dws_alct_clean_smooth_sales_replenishment_ds
	where dt = date_sub(curdate(), interval 1 day)
	group by sku, platform_id, site_id
)
, year_max_sales as (
	select
	    dt, sku, platform_id, site_id
	    , round(clean_avg_sales_7d, 2) as year_max_avg_sales_7d
		from(
		select *, row_number() over(
		    partition by sku, platform_id, site_id
		    order by clean_avg_sales_7d desc, dt) as rn
		from(
			select dt
			    , sku, platform_id, site_id
				, sum(clean_avg_sales_7d) as clean_avg_sales_7d
			from dws.dws_alct_clean_smooth_sales_replenishment_ds
			where dt between  date_sub(curdate(), interval 1 year) and date_sub(curdate(), interval 1 day)
			group by dt, sku, platform_id, site_id
		) as t1
	) as t2
	where rn = 1
)
, sales_channel as (
    select sku, platform_id, site_id
        , platform_name, site_name
        , sale_status, department_3_name
    from dwd.dwd_dim_sales_channel_ds
    where dt = date_sub (curdate(), interval 1 day)
        and sale_status = '在售'
        and department_3_name = '园艺精品部'
)
, rp_sales as (
    select sku, platform_id, site_id, daily_sales as rp_sales
    from dwd.dwd_fact_pcct_rp_sales_td_df
    where dt = curdate()
    and start_date = dt
)
select
    t.sku, sc.platform_name 平台, sc.site_name 站点
    , ifnull(max(rp_sales), 0) as 今日未来补货销量
	, ifnull(max(avg_sales_7d), 0) as 近7天日均
	, ifnull(max(smooth_sales_short), 0) as `7/14/28平滑`
	, ifnull(max(smooth_sales), 0) as `30/60/90平滑`
	, ifnull(max(year_max_avg_sales_7d), 0) as 近一年最高7天日均销量
	, ifnull(max(year_max_dt), '') as 近一年最高7天日均销量日期
from(
	select sku, platform_id, site_id,avg_sales_7d,  smooth_sales_short
	, smooth_sales, 0 as year_max_avg_sales_7d, null as year_max_dt
	, 0 as rp_sales
	 from smooth_sales
	union all
	select sku, platform_id, site_id, 0 as avg_sales_7d, 0 as smooth_sales_short
	, 0 as smooth_sales, year_max_avg_sales_7d, dt as year_max_dt
	,0 as rp_sales
	from year_max_sales
	union all
	select sku, platform_id, site_id, 0 as avg_sales_7d, 0 as smooth_sales_short
	, 0 as smooth_sales,0 year_max_avg_sales_7d, null as year_max_dt
	, rp_sales
	from rp_sales
) as t
join sales_channel as sc on sc.sku = t.sku
	and sc.platform_id = t.platform_id
	and sc.site_id = t.site_id
group by t.sku, sc.platform_name, sc.site_name
order by t.sku, sc.platform_name, sc.site_name
;