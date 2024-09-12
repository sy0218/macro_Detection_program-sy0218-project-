drop table keylog;
CREATE TABLE keylog_raw (
    key_log_time STRING,
    key_active STRING,
    user_id STRING
)
PARTITIONED BY (pdate STRING)
STORED AS PARQUET;
