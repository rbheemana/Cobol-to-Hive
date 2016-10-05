use ${hiveconf:hv_db_stage};
DROP TABLE IF EXISTS ${hiveconf:stage_table};

 CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:hv_db_stage}.${hiveconf:stage_table} (
    ${hiveconf:createStgColumns}
 )
--LOCATION '${hiveconf:stg_target_dir}'
 ;

