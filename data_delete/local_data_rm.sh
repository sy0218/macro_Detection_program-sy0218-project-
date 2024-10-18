#!/usr/bin/bash

# 인자 두개인지 확인
if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <로컬 경로> <보존 기간>"
        exit 1
fi

rm_path=$1
preservation_period=$2

find ${rm_path} -type d -mtime +${preservation_period} -exec rm -rf {} +
find ${rm_path} -type f -mtime +${preservation_period} -exec rm -rf {} +
