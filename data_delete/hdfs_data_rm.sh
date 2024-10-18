#!/usr/bin/bash

# 인자 두개인지 확인
if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <하둡 경로> <보존 기간>"
        exit 1
fi

rm_path=$1
preservation_period=$2

hdfs dfs -ls -R ${rm_path} | awk -v date="$(date -d "${preservation_period} days ago" +%Y-%m-%d)" '$6 < date {print $8}' | xargs -r hdfs dfs -rm -r
