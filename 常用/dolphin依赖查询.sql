use dolphinscheduler_test;
select p.name, b.name,c.name, b.task_params
from t_ds_process_task_relation as a 
join t_ds_task_definition as b on b.code = a.post_task_code
left join t_ds_task_definition as c on c.code = a.pre_task_code
join t_ds_process_definition as p on p.code = a.process_definition_code
where  p.name = 'default'
and b.name = 'ods_md_me_first_trip_performance_param_df'
-- and c.name = 'dwd_fact_ivct_batch_stock_flowing_di'
;


select min(id)
from t_ds_process_instance
where start_time >= curdate()
;
delete from t_ds_process_instance where id < 71391;
select count(1) from t_ds_task_instance 
;
select count(1) from t_ds_process_task_relation_log
;
truncate table t_ds_process_task_relation_log;
insert into t_ds_process_task_relation_log(
name
,project_code
,process_definition_code
,process_definition_version
,pre_task_code
,pre_task_version
,post_task_code
,post_task_version
,condition_type
,condition_params
,operator
,operate_time
,create_time
,update_time
)
select 
  a.name
  ,a.project_code
  ,a.process_definition_code
  ,a.process_definition_version
  ,a.pre_task_code
  ,a.pre_task_version
  ,a.post_task_code
  ,a.post_task_version
  ,a.condition_type
  ,a.condition_params
  ,b.user_id as operator
  ,a.update_time as operate_time
  ,a.create_time
  ,a.update_time
from t_ds_process_task_relation as a 
join t_ds_process_definition as b on b.code = a.process_definition_code
;
delete from t_ds_process_definition_log 
where code not in ( select code from t_ds_process_definition)
;
select count(1)
from t_ds_task_definition_log 
;
select count(1)
from t_ds_task_definition
;