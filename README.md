# 키 로그 카프카 프로듀서

## 필수 준비

- **필수 셋팅**: `정리 블로그 https://sy02229.tistory.com/406(kafka 설치, 토픽 생성 등..)`

## 각 파일 설명

1. **`play_get_log.py`** : `각 사용자 keg log 수집을 위한 카프카 프로듀서`
2. **`play_zone/play.sh`** : `특정 조건에 키로그를 수집 하기위한 트리거`
3. **`make_topic/create_keylog_topic.sh`** : `카프카 일별 토픽 관리를위해 key_log 토픽 관리 및 생성하는 스크립트`
4. **`kafka_consumer/mp_kafka_consumer.py`** : `토픽 파티션 갯수에 만큼 컨슈머를 병렬처리해 row_data 디렉토리에 일자별 log 데이터 저장하는 컨슈머 생성 스크립트`
5. **`row_data/mp_keylog_{YYYYMMDD}.txt`** : `key_log의 원시데이터가 저장되는 디렉토리`

## 실행 방법
1. `make_topic/create_keylog_topic.sh` 오늘 일자 넣어주기(YYYMMDD) 형식
   ```sh
   ex) ./create_keylog_topic.sh {today_date}
   ```
2. `kafka_consumer/mp_kafka_consumer.py` bootstrap_servers, topic, output_file, consum_group 인자 넣어주기
   ```sh
   ex) ./mp_kafka_consumer.py 192.168.56.10:9092,192.168.56.11:9092,192.168.56.12:9092 key_log_20240830 /data/keyboard_sc/row_data/mp_keylog_20240830.txt mp_keylog_20240830
   ```
