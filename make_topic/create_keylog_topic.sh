#!/usr/bin/bash

# 인자 개수 확인
if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <date>"
        exit 1
fi

# 레플리케이션2, 파티션3 ( 차후 컨슈머 병렬 처리 위해 )
# 현재 날짜를 저장
now_dt=$1

# 브로커 IP와 포트를 배열로 정의
BROKERS=("192.168.56.10" "192.168.56.11" "192.168.56.12")
PORT=9092
TOPIC="key_log_${now_dt}"

# 모든 브로커에 대해 포트 열림 여부를 확인
for BROKER in "${BROKERS[@]}"; do
	if ! nc -z "$BROKER" "$PORT"; then
		echo "Error: Unable to connect to kafka server $BROKER:$PORT" >&2
		exit 1
	else
		echo "Success: Connected to kafka server $BROKER:$PORT"
	fi
done

# 토픽 존재 여부 확인
if kafka-topics.sh --list --bootstrap-server "${BROKERS[0]}:$PORT" | grep -q "^$TOPIC$"; then
	echo "Topic $TOPIC already exists. Exiting."
	exit 0
fi


# kafka 토픽 생성
kafka-topics.sh --create --topic "${TOPIC}" \
--partitions 3 \
--replication-factor 2 \
--bootstrap-server "${BROKERS[0]}:$PORT"

# 정상 실행 확인
if [ $? -eq 0 ]; then # $? >>> 마지막 명령어 종료 상태 확인하는 특별한 변수
	echo "kafka topic key_log_${now_dt} created"
else
	echo "Failed created kafka topic key_log_${now_dt}" >&2
	exit 1
fi
