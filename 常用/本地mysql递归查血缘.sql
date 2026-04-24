-- 核心修改：WITH后加RECURSIVE，并重命名冲突的t_lineage为filtered_lineage
WITH RECURSIVE filtered_lineage AS (
    -- 过滤指定的source-target组合（原t_lineage逻辑）
    select source, target
    from t_lineage  -- 这里是物理表，无冲突
    where source <> target
    and not (target = 'dws_ivct_piece_fcl_base_ds' and source = 'dws_ivct_stock_health_label_di')
),
cte AS (
    -- 递归起点：指定源头表，初始层级为1
    select 
        source, 
        target, 
        1 as level,
        CONCAT(source, ' -> ', target) as lineage_path  -- 记录血缘路径，防循环
    from filtered_lineage 
    where source = 'dws_ivct_stock_health_assessment_di'
    
    union all 
    
    -- 递归查询下游：避免循环引用
    select 
        a.target as source, 
        b.target, 
        a.level + 1,
        CONCAT(a.lineage_path, ' -> ', b.target) as lineage_path
    from cte as a
    join filtered_lineage as b on a.target = b.source 
    -- 关键：防止循环递归（路径中不含当前目标表）
    where INSTR(a.lineage_path, b.target) = 0
    -- 兜底：限制最大层级，避免无限递归
    and a.level < 100
)
select 
    source, 
    target, 
    level,
    lineage_path
from cte 
order by level, source, target;