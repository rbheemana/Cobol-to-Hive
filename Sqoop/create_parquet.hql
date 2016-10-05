
use ${hiveconf:hv_db};

--This can be commented after development and before final unit testing
DROP TABLE IF EXISTS ${hiveconf:table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:hv_db}.${hiveconf:table} (  
  ${hiveconf:columnsWithoutPartition}
  ,hdfs_load_ts timestamp
) 
 ${hiveconf:partitionedBy}
 STORED AS PARQUET
-- LOCATION '${hiveconf:target_dir}'
TBLPROPERTIES("parquet.compression"="SNAPPY");

--msck repair table ${hiveconf:hv_db}.${hiveconf:table};
msck repair table ${hiveconf:table};

