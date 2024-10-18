#!/usr/bin/bash

# 인자 두개인지 확인
if [ "$#" -ne 3 ]; then
        echo "Usage: $0 <배치 날짜> <aws 버킷> <보존 기간>"
        exit 1
fi

batch_date=$1
s3_path=$2
preservation_period=$3

rm_date=$(date -d "${batch_date} - ${preservation_period} days" +%Y%m%d)

s3_path="s3://${s3_path}"
aws s3 ls "${s3_path}" --recursive | while read -r line;
do
        file_size=$(echo ${line} | awk '{print $3}')
        if [ ${file_size} -gt 0 ]; then
                file_path=$(echo ${line} | awk '{print $4}')
                file_date=$(echo ${line} | awk '{print $1}' | sed 's/-//g')

                # 보존 정책 보다 작은 경우 제거
                if [ ${file_date} -lt ${rm_date} ]; then
                        echo "삭제파일 ${file_path}"
                        aws s3 rm ${s3_path}/${file_path}
                fi
        fi
done
