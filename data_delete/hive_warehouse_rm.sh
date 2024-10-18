#!/usr/bin/bash

# 인자 두개인지 확인
if [ "$#" -ne 4 ]; then
        echo "Usage: $0 <배치 날짜> <hive 테이블 명> <보존 기간> <파티션 명>"
        exit 1
fi


rm_table_name=$2
preservation_period=$3
partition_name=$4

rm_date=$(date -d "$1 -${preservation_period} days" +%Y%m%d)
# 삭제할 파티션 목록 가져옴
hive_rm_partitions=$(hive -e "select distinct ${partition_name} from ${rm_table_name} where ${partition_name} < ${rm_date}")

for partition in $hive_rm_partitions;
do
        echo "삭제 파티션: ${partition_name}=${partition}"
        hive -e "alter table ${rm_table_name} drop if exists partition (${partition_name}='${partition}')"
done
