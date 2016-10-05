set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

INSERT OVERWRITE TABLE ${hv_db}.${table}
  ${partition_clause}
SELECT  
  ${columns_without_partition}
 ,${hdfs_load_ts}
  ${partition_column_select}
FROM ${hv_db_stage}.${stage_table};
 
--msck repair table ${hv_db}.${table};
