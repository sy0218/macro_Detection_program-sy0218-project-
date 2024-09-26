#!/usr/bin/bash

hadoop_dir=$1
batch_date=$2
file_name=$3
hive_table=$4

# hive 웨어하우스 적재
hive -e "set mapred.job.priority=VERY_HIGH;load data inpath '${hadoop_dir}/${file_name}' overwrite into table ${hive_table} partition (pdate='${batch_date}')"
