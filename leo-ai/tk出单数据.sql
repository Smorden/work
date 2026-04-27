/*
# 需求
1. 近 3–6 个月 TK 销售占比变化
2. 新品上线 30 天出单率
3. 60 天持续出单率
4. 按类目统计：销量分组区间占比
5. 刊登时库存
6. 爆单后补货风险案例
# 输出
- 销售额
	- 月份，TK销售额，总销售额，占比
- 明细
	- sku，站点，首次刊登日期，首次出单日期，首次出单天数
	- 维度补充：pm，op，部门，中文名
	- 一级类目，二级类目，产品等级，日均平滑销量，销量分组
	- 刊登库存
	- 新品基准销量，爆单日期，爆单日7天日均销量，爆单日4个月后的平滑销量
*/
-- SQL
-- 销售额
select
date_format(dt, '%Y年%m月') as 年月
, sum(if(platform_id = 31, sales_amt_cny_fi, 0)) as TK销售额
, sum(sales_amt_cny_fi) as 总销售额
, sum(if(platform_id = 31, sales_amt_cny_fi, 0)) / sum(sales_amt_cny_fi) as TK销售额占比
from ads.ads_alct_theory_profit_account_details_di
where dt between date_sub(curdate(), interval 6 month) and date_sub(curdate(), interval 1 day)
group by date_format(dt, '%Y年%m月')
order by date_format(dt, '%Y年%m月')
;
-- 明细
drop TABLE if exists temp.temp_leo_tiktok_list_sale_sku_ds ;
CREATE TABLE if not exists temp.temp_leo_tiktok_list_sale_sku_ds (
    dt date 
  , sku varchar(100) NULL COMMENT 'sku'
  , site_id int NULL COMMENT '站点id'
  , site_name varchar(10) NULL COMMENT '站点的英文简称'
  , min_list_dt date NULL
  , sale_status varchar(50) NULL COMMENT '销售等级(状态)'
  , charge_user_name varchar(50) NULL COMMENT '(开发)负责人姓名'
  , channel_user_name varchar(50) NULL COMMENT '运营负责人'
  , department_3_name varchar(100) NULL COMMENT '部门3name'
  , sku_name string NULL COMMENT '中文名称'
  , lv1_category_en_name string NULL COMMENT '一级英文类目'
  , lv2_category_en_name string NULL COMMENT '二级英文类目'
  , lv3_category_en_name string NULL COMMENT '三级英文类目'
  , min_sale_dt date NULL comment '首次刊登日期'
  , clean_qty_smooth decimalv3(26, 8) NULL COMMENT '清洗28-56-91平滑销量'
  , range_concat varchar(50) NULL COMMENT '平滑销量分组'
  , list_stock_qty bigint(20) COMMENT '刊登库存'
  , explode_dt date NULL COMMENT '爆单日期'
  , avg_sales decimalv3(26, 8) NULL COMMENT '爆单日7天日均销量'
  , last_avg_sales decimalv3(26, 8) NULL COMMENT '爆单日7天前的7天日均销量'
  , futrue_dt date COMMENT '爆单后未来日期'
  , futrue_avg_sales decimalv3(26, 8) COMMENT '爆单后未来日均销量'
  , list_sale_days int comment '出单天数'
  , etl_create_time    DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'etl执行时间'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT 'leo：tk刊登销售sku明细'
	PARTITION BY RANGE(`dt`)(
		PARTITION p_19700101 VALUES LESS THAN ( "2026-04-01" ),
		FROM ( '2026-04-01' ) TO ( @dt ) INTERVAL 1 DAY
	    )
	DISTRIBUTED BY HASH(sku,site_id) BUCKETS 3
	PROPERTIES (
		"dynamic_partition.enable" = "true",
		"dynamic_partition.time_unit" = "DAY",
		"dynamic_partition.end" = "3",
		"dynamic_partition.prefix" = "p_",
		"dynamic_partition.create_history_partition" = "true",
		"dynamic_partition.history_partition_num" = "7",
		"dynamic_partition.buckets" = "1",
		"light_schema_change" = "true"
	);
truncate table temp.temp_leo_tiktok_list_sale_sku_ds partition(p_2026 );
insert into temp.temp_leo_tiktok_list_sale_sku_ds(
  dt
, sku
, site_id
, site_name
, min_list_dt
, sale_status
, charge_user_name
, channel_user_name
, department_3_name
, sku_name
, lv1_category_en_name
, lv2_category_en_name
, lv3_category_en_name
, min_sale_dt
, clean_qty_smooth
, range_concat
, list_stock_qty
, explode_dt
, avg_sales
, last_avg_sales
, futrue_dt
, futrue_avg_sales
, list_sale_days
)
with min_list_dt as(
    select tl.sku, tl.site_id
        , max(tl.site_name) as site_name
        , date(date_add(min(tp.product_create_time_utc), interval 8 hour)) as min_list_dt
    from dwd.dwd_dim_opct_tiktok_listing_ds as tl
    join ods.ods_pbbs_api_tiktok_products_df as tp on tl.tiktok_product_id = tp.id
    group by tl.sku, tl.site_id
)
, site_stock as (
    select ws.dt, ws.sku, wh.warehouse_site_id as site_id, sum(stock_qty) as stock_qty
    from (
        select dt, sku, warehouse_id, stock_qty
        from dws.dws_ivct_goods_warehouse_stock_ds
        where dt >= '2024-05-01'
            and stock_stage = '海外仓在库'
        ) as ws
    join dwd.dwd_dim_warehouse_df as wh on wh.warehouse_id = ws.warehouse_id
    group by ws.dt, ws.sku, wh.warehouse_site_id
)
, sales_channel as(
    select sku, site_id, sale_status, channel_user_name, charge_user_name, department_3_name
    from dwd.dwd_dim_sales_channel_ds
    where dt = date_sub(curdate(), interval 1 day)
        and platform_id = 31
)
, min_sale_dt as(
    select sku, site_id, min(dt) as min_sale_dt
    from dws.dws_alct_theory_profit_sku_plat_site_di
    where platform_id = 31 and sales_qty > 0
    group by sku, site_id
)
, dim_sku as (
    select sku, sku_name, lv1_category_en_name, lv2_category_en_name, lv3_category_en_name
        , charge_user_name, department_3_name
    from dwd.dwd_dim_sku_ds
    where dt = date_sub(curdate(), interval 1 day)
)
, ss_range as (
    select cast(0 as int) as range_start union all
    select 1 as range_start union all
    select 5 as range_start union all
    select 10 as range_start union all
    select 50 as range_start union all
    select 100 as range_start
)
, ss_range_full as(
    select range_start, range_end
        , concat(range_start
            , ' ~ '
            , range_end
            ) as range_concat
    from(
        select range_start
            , lead(range_start, 1, 99999) over(order by range_start) as range_end
        from ss_range
    ) as t
)
, smooth_sales as(
    select sku, site_id
        , clean_qty_smooth
    from dws.dws_alct_clean_smooth_sales_replenishment_ds
    where dt = date_sub(curdate(), interval 1 day)
        and platform_id = 31
        and is_low_profit = 0
)
, black_days as (
    select b.date_id as black_days
    from(
    select '2024-11-29' as black_friday
    union all select '2025-11-28'
    ) as a
    join dwd.dwd_dim_date_nf as b
        on b.date_id between a.black_friday and date_add(a.black_friday, interval 9 day)
)
, sales_base as (
    select dt, sku, site_id, clean_avg_sales_7d, sale_status
    from dws.dws_alct_clean_smooth_sales_replenishment_ds
    where is_low_profit = 0
        and platform_id = 31
)
, explode_days as (
    select a.dt, a.sku, a.site_id, a.clean_avg_sales_7d as avg_sales
        , b.clean_avg_sales_7d as last_avg_sales
        , row_number() over(
            partition by a.sku, a.site_id
            order by a.clean_avg_sales_7d desc
            ) as rn
        , count(1) over(partition by a.sku, a.site_id) as days_cnt
    from sales_base as a
    join sales_base as b on b.sku = a.sku and b.site_id = a.site_id
        and b.dt = date_sub(a.dt, interval 7 day)
        and b.sale_status = '在售'
    where a.dt not in (select black_days from black_days)
        and a.clean_avg_sales_7d >= b.clean_avg_sales_7d * 5
        and a.clean_avg_sales_7d >= 5
        and b.clean_avg_sales_7d > 0
        and a.sale_status = '在售'
)
, explode_futrue_sales as (
    select
        ed.dt as explode_dt, ed.sku, ed.site_id
        , ed.avg_sales, ed.last_avg_sales
        , sb.sale_status
        , if( date_add(ed.dt, interval 120 day) >= curdate(), null
            , ifnull(sb.clean_avg_sales_7d, 0)) as futrue_avg_sales
				, ed.futrue_dt
    from (
        select *, date_add(ed.dt, interval 120 day) as futrue_dt from explode_days where rn = 1 and days_cnt > 2
    ) as ed
    left join sales_base as sb on sb.sku = ed.sku and sb.site_id = sb.site_id
        and sb.dt =ed.futrue_dt
)
select
     curdate() as dt
    , ml.sku, ml.site_id, st.site_name, ml.min_list_dt
    , sc.sale_status, sku.charge_user_name, sc.channel_user_name, sc.department_3_name
    , replace(sku.sku_name, '\t', '') as sku_name
    , sku.lv1_category_en_name, sku.lv2_category_en_name, sku.lv3_category_en_name
    , ms.min_sale_dt
    , ss.clean_qty_smooth
    , sr.range_concat
    , ifnull(sk.stock_qty, 0) as list_stock_qty
    , ef.explode_dt
    , ef.avg_sales
    , ef.last_avg_sales
		, ef.futrue_dt
    , ef.futrue_avg_sales
    , if(datediff(min_sale_dt, min_list_dt) < 0, 0, datediff(min_sale_dt, min_list_dt)) as list_sale_days
from min_list_dt as ml
join dim_sku as sku  on sku.sku = ml.sku
join dwd.dwd_dim_site_df as st on st.site_id = ml.site_id
join sales_channel as sc on ml.sku = sc.sku and ml.site_id = sc.site_id
left join min_sale_dt as ms on ms.sku = ml.sku and ms.site_id = ml.site_id
left join smooth_sales as ss on ss.sku = ml.sku and ss.site_id = ml.site_id
left join ss_range_full as sr on sr.range_start < ss.clean_qty_smooth and ss.clean_qty_smooth <= sr.range_end
left join site_stock as sk on sk.sku = ml.sku and sk.site_id = ml.site_id and sk.dt = ml.min_list_dt
left join explode_futrue_sales as ef on ef.sku = ml.sku and ef.site_id = ml.site_id
;
select
  sku as sku
, site_name as 站点
, sale_status as 产品等级
, charge_user_name as PM
, channel_user_name as OP
, department_3_name as 部门
, sku_name as 中文名
, lv1_category_en_name as 一级类目
, lv2_category_en_name as 二级类目
, lv3_category_en_name as 三级类目
, min_list_dt as 首次刊登日期
, min_sale_dt as 首次出单日期
, list_sale_days as 出单天数
, clean_qty_smooth as 当前平滑销量
, range_concat as 平滑销量分组
, list_stock_qty as 刊登日库存
, explode_dt as 爆单日期
, avg_sales as 爆单日均销量
, last_avg_sales as 爆单7天前日均销量
, futrue_dt as 爆单4个月后日期
, futrue_avg_sales as 爆单4个月后日均销量
from temp.temp_leo_tiktok_list_sale_sku_ds
where dt = curdate()
order by min_list_dt, sku, site_name
;
select list_sale_days as 出单天数
    , sku_cnt as sku数
    , sum(sku_cnt) over(order by list_sale_days) as 累加sku数
    , total_sku_cnt as 总sku数
    , sum(sku_cnt) over(order by list_sale_days)/total_sku_cnt as 累加出单率
from(
select
	if(list_sale_days >= 180, 180, list_sale_days) as list_sale_days
    , count(1) as sku_cnt
from temp.temp_leo_tiktok_list_sale_sku_ds
where dt = curdate() and list_sale_days is not null
group by if(list_sale_days >= 180, 180, list_sale_days)
) as a
join (select count(1) as total_sku_cnt from temp.temp_leo_tiktok_list_sale_sku_ds) as b
order by list_sale_days
;
select
    lv1_category_en_name 一级类目
    , lv2_category_en_name 二级类目
    , lv3_category_en_name 三级类目
    , ifnull(range_concat, '0 ~ 1') 销量区间
    , count(1) as sku数
	, sum(ifnull(clean_qty_smooth, 0)) as 总销量
from temp.temp_leo_tiktok_list_sale_sku_ds
where dt = curdate()
    and sale_status in ('停购', '在售')
group by  lv1_category_en_name
    , lv2_category_en_name
    , lv3_category_en_name
    ,  ifnull(range_concat, '0 ~ 1')
order by lv1_category_en_name
    , lv2_category_en_name
    , lv3_category_en_name
    ,  ifnull(range_concat, '0 ~ 1')
;