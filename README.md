# 매크로 탐지 시스템
![제목을-입력해주세요_-001](https://github.com/user-attachments/assets/00cebe44-facd-4a20-9ba3-0f108bc4a5e0)  
- **유저별 키보드 로그를 수집하고 배치처리를 통한 매크로를 사용한 유저 탐지하는 시스템**  
<br><br>
---  
<br><br>
## 🚀 사용 기술  
- **분산 서버 배포**: `vagrant`
- **코드 관리**: `git`
- **운영 환경을 위한 애플리케이션 설치 및 setup**: `ansible`
- **키보드 로그 실시간 스트리밍**: `kafka`
- **분산 환경을 위한 애플리케이션**: `hadoop`, `hive`, `zookeeper`, `spark`
- **데이터 베이스**: `postgresql`
- **워크플로우 도구**: `airflow`
- **컨테이너 도구**: `docker-compose`, `docker`
- **간단한 web**: `flask`
- **매크로 탐지 모델**: `lstm`
- **외부 저장소**: `AWS S3`  
<br><br>
---  
<br><br>
## 📦 Kafka  
- 유저가 `play.sh` 실행 시 키보드 로그 수집, 종료 시 수집 종료합니다.
- **Kafka 브로커 운영 서버**: 3대 (가상환경)
- **Kafka 브로커 운영 서버 Crontab**: 매 23시에 다음 날짜의 토픽 생성합니다.
   ```sh
   0 23 * * * /data/keyboard_sc/make_topic/create_keylog_topic.sh $(date -d tomorrow +"%Y%m%d") >> /data/keyboard_sc/make_topic/create_topic.log 2>&1
   ```
### (2-1) 카프카 프로듀서
```python
#!/usr/bin/python3
from evdev import InputDevice, categorize, ecodes, list_devices
import os
import subprocess
import time
from datetime import datetime

from kafka import KafkaProducer

def input_events(broker, topic, username):
    dev = InputDevice('/dev/input/event2')
    producer = KafkaProducer(bootstrap_servers=broker) # 프로듀서 객체 생성

    try:
        for event in dev.read_loop():
            event_str = str(categorize(event))
            if ("key event at" in event_str) and ("down" in event_str):
                user_key_log = event_str + '\t' + str(username)
                producer.send(topic, user_key_log.encode('utf-8'))
                producer.flush() # 메시지 바로 전송
    finally:
        producer.close() # 객체 종료

if __name__ == "__main__":
    username=os.getenv('USER')
    today=datetime.now().strftime('%Y%m%d')
    broker_address = ['192.168.219.10:9092', '192.168.219.11:9092', '192.168.219.12:9092'] # kafka 브로커 주소
    topic_name = f"key_log_{today}" # 토픽명
    input_events(broker_address, topic_name ,username)
```
- **해당 프로듀서를 통해 키보드 입력 로그 kafka 브로커로 전송**  
<br><br>
---  
<br><br>
## 🛠️ Airflow  
- **매 00시 워크플로우 배치처리 파이프라인 실행**
- **Airflow 운영서버 Crontab : 매 00시 카프카 컨슈머 병렬 실행 스크립트를 통해 전일자 키보드 로그 가져오기**
  ```sh
  0 0 * * * /usr/bin/python3 /data/keyboard_sc/kafka_consumer/mp_kafka_consumer.py 192.168.219.10:9092,192.168.219.11:9092,192.168.219.12:9092 key_log_$(date +\%Y\%m\%d) /data/keyboard_sc/row_data/keylog/$(date +\%Y\%m\%d)/mp_keylog_$(date +\%Y\%m\%d).txt mp_keylog_$(date +\%Y\%m\%d) >> /data/keyboard_sc/kafka_consumer/kafka_consumer.log 2>&1
  ```
### (3-1) Airflow 파이프라인
<img width="1101" alt="airflow" src="https://github.com/user-attachments/assets/521a07b4-bfed-4ca3-9faf-c5414bd21223">

<br>  

- **각 task 설명**


1. **`push_xcom`** : airflow 파이프라인 변수를 위한 xcom push
2. **`wait_keylog_rowdata`** : 카프카 컨슈머를 통핸 원시 데이터 수집 완료까지 대기하는 센서
3. **`preprocessing_use_spark`** : pyspark를 통한 원시 데이터 전처리
4. **`to_hive`** : 전처리 완료된 원시데이터 웨어하우스(hive)에 적재
5. **`detection_macro`** : 정재된 데이터를 가지고 매크로 탐지 프로그램 적용후 최종본 postgresql에 적재 
6. **`row_data_zip`** : 원시 데이터 압축 (gzip)
7. **`row_data_zip`** : 전처리 데이터 압축 (gzip)
8. **`row_zipfile_to_s3`** : 압축된 원시데이터 AWS S3 적재(외부 저장소)
9. **`row_zipfile_to_hdfs`** : 압축된 원시데이터 hdfs 적재(내부 저장소)
10. **`process_zipfile_to_s3`** : 압축된 전처리 데이터 AWS S3 적재(외부 저장소)
11. **`process_zipfile_to_hdfs`** : 압축된 전처리 데이터 hdfs 적재(내부 저장소)
12. **`local_zipfile_remove`** : 압축 데이터 모두 적재 완료 후 삭제
13. **`check_and_insert_consistency`** : 배치처리된 데이터 검증을 위한 초기값 postgresql에 삽입
14. **`raw_data_line_count`** : 원시데이터 raw수 카운트
15. **`hive_data_line_count`** : 전처리리데이터 raw수 카운트
16. **`raw_data_awk_count`** : 원시데이터 필드수 카운트
17. **`decode ~~~`** : 검증을 위한 카운트된 값 디코딩
18. **`update ~~~`** : 구한 카운트 값으로 postgresql의 검증테이블 update
19. **`rm ~~~`** : 데이터 보존 정책에 따라 데이터 삭제 (local,hive 7일) (hdfs, s3 백업 10일)  
<br><br>
---  
<br><br>
## 🔍 매크로 탐지 프로그램  
```python
#!/usr/bin/python3
from postgres.postgres_hook import PostgresConnector
import sys
import psycopg2
from psycopg2 import sql
from pyhive import hive
import pandas as pd
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pickle

# 키로그 함수 불러오기
from keylog_functions.keylog_ft import _key_time_ft

def main(batch_date, version):
    conn = hive.Connection(host='192.168.219.20', port=10000, username='sy0218', database='default')
    # 쿼리 실행 (작은 따옴표 이스케이프 처리)
    query = f"SELECT * FROM keylog_raw WHERE pdate='{batch_date}' and user_id is not null"
    df = pd.read_sql(query, conn)
    conn.close()

    # 필요 컬럼만 가져온후 정렬
    df = df[['keylog_raw.key_log_time','keylog_raw.key_active','keylog_raw.user_id']].sort_values(by=['keylog_raw.user_id','keylog_raw.key_log_time'], ascending=[True,True])

    group_df = df.groupby('keylog_raw.user_id').agg({
        'keylog_raw.key_log_time': lambda x: list(x.astype(float)),
        'keylog_raw.key_active': lambda x: list(x)
    }).reset_index()
    macro_ai_df = group_df[['keylog_raw.key_active','keylog_raw.user_id']]

    # 모델과 토크나이저 로드
    model_path = f"/app/model_materials/macro_model_{version}.keras"
    tokenizer_path = f"/app/model_materials/macro_tokenizer_{version}.pkl"
    model = load_model(model_path)
    with open(tokenizer_path, 'rb') as tk:
        tokenizer = pickle.load(tk)

    # key_active 시퀀스 데이터를 토큰으로 변환 (max_length는 50, 패딩은 post)
    max_length = 50
    def preprocess_key_active(key_active_sequences):
        X = [[tokenizer.get(key, 0) for key in seq] for seq in key_active_sequences]
        return pad_sequences(X, maxlen=max_length, padding='post')

    # key_active 토큰 변환( 전처리 )후 예측
    pred_macro_ai = model.predict(preprocess_key_active(macro_ai_df['keylog_raw.key_active'].tolist()))
    pred_df = pd.DataFrame(pred_macro_ai)
    pred_df.columns = ['pred_result']
    combi_df = pd.concat([group_df, pred_df], axis=1)

    # 키 입력 텀 컬럼 추가
    combi_df['diff_key_log_time'] = group_df['keylog_raw.key_log_time'].apply(lambda x: [0 if i==0 else round(x[i] - x[i-1], 2) for i in range(len(x))])
    # 동일한 키 입력한 텀중 최대 카운트 return ( 매크로시 동일 간견으로 키를 누르니까 카운트가 높을것.. )
    combi_df['key_time_pc'] = combi_df['diff_key_log_time'].apply(_key_time_ft)
    result_df = combi_df[['keylog_raw.user_id','pred_result','key_time_pc']]
    result_df['pdate'] = batch_date
    print(result_df)

    # 커서 생성
    postgres_conn = PostgresConnector.get_connection('ps_20')
    pg_cr = postgres_conn.cursor()

    # 멱등성 위해 기존 데이터 삭제
    pg_cr.execute(f"DELETE FROM macro_pg_result where pdate='{batch_date}'")
    # 데이터 삽입
    for index, row in result_df.iterrows():
        pg_cr.execute(
            sql.SQL("INSERT INTO macro_pg_result (user_id, pred_result, key_time_pc, pdate) VALUES (%s, %s, %s, %s)"),
            (row['keylog_raw.user_id'], row['pred_result'], row['key_time_pc'], row['pdate'])
        )

    # 커밋 및 연결 종료
    postgres_conn.commit()
    pg_cr.close()
    PostgresConnector.dis_connection() # postgres 종료

if __name__ == "__main__":
    args = sys.argv[1:]
    # 인자가 두개 아니면 종료
    if (len(args) != 2):
        print("사용법 : py <YYYYmmdd> <객체 버전>")
        sys.exit(1)
    batch_date, version = args
    main(batch_date, version)
```
- **사용자 별로 두 값을 리턴**
- **lstm 모델을 통해 예측 결과 return ( 1 에 가까울수록 매크로일 가능성 높음 )**
- **키 입력한 동일 시간 텀의 개수를 카운트한 값의 최댓값을 통해 키 로그 텀 비율 return( 최댓값/전체값 * 100 )**
- **즉, 모델 탐지 결과와 동일 키 로그 텀 비율이 높을 수록 매크로 사용자일 것으로 판단할수 있습니다!!**  
<br><br>
---  
<br><br>
## ⭐ docker-compose  
```sh
version: '3.7'
######### airflow env #########
x-environment: &airflow_environment
    AIRFLOW__CORE__EXECUTOR: "SequentialExecutor"
    AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS: "False"
    AIRFLOW__CORE__LOAD_EXAMPLES: "False"
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: "sqlite:////opt/airflow/airflow.db"
    AIRFLOW__CORE__STORE_DAG_CODE: "True"
    AIRFLOW__CORE__STORE_SERIALIZED_DAGS: "True"
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "True"
    AIRFLOW__WEBSERVER__RBAC: "False"
    AIRFLOW_CONN_APSERVER_SSH: "ssh://root:nds1101@192.168.219.20:22"
    AIRFLOW_CONN_APSERVER_POSTGRES: "postgresql://sy0218:sy0218@192.168.219.20:5432/sy0218"
######### airflow env #########
services:
  airflow:
    build: /data/keyboard_sc/work_airflow/docker_images/docker_airflow
    image: airflow/airflow_sy0218:2.9.3
    container_name: airflow_container
    ports:
      - "9898:8080"
    volumes:
      - /data/keyboard_sc/work_airflow/dag_airflow/:/opt/airflow/dags/
      - /data/keyboard_sc/work_airflow/pem_dir/:/opt/airflow/pem_dir/
      - /data/keyboard_sc/row_data:/data/keyboard_sc/row_data
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - keylog_airflow
    environment: *airflow_environment

  macro_detection_program:
    build: /data/keyboard_sc/macro_detection_program/docker_images/exe_macro_program
    image: exe_macro_dt_pg:1
    container_name: Macro_Detection_Program
    restart: "no"

  make_macro_detection_program_model:
    build: /data/keyboard_sc/macro_detection_program/docker_images/mk_macro_program
    image: macro_dt_pg:0.0.1
    container_name: Make_Macro_Detection_Program_Model
    restart: "no"

  flask_macro_application:
    build: /data/keyboard_sc/flask_macro
    image: flask_macro_app:0.0.1
    container_name: macro_app_container
    ports:
      - "7777:5000"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - keylog_airflow

networks:
  keylog_airflow:
    name: keylog_airflow
```
- **각 애플리케이션 별 격리를 위해 4가지 컨테이너를 구현 하였습니다.**  
<br><br>
---  
<br><br>

## 🌐 UI 설명  

### (6-1) 메인 페이지
<img width="1055" alt="ui_main" src="https://github.com/user-attachments/assets/b3a80079-2003-431f-84c6-f0ea7fee8829">

- **특정 날짜의 매크로 탐지 결과와 데이터 검증관련 정보를 확인할수 있습니다.**  

### (6-2) 모델 생성 메뉴
<img width="412" alt="model_m_page" src="https://github.com/user-attachments/assets/a4399072-a2ac-4733-a385-264bd3a2d709">

- **담당자가 직접 모델 버전을 입력후 생성할수 있습니다.**

### (6-3) 에어플로우 UI 메뉴
<img width="1259" alt="airflow_ui" src="https://github.com/user-attachments/assets/f0ee04cf-5676-4992-8365-c28ddaafd726">

- **Airflow UI에 접속할수 있는 메뉴 입니다.**  

<br><br>
---  
<br><br>

## 🖥️ 운영서버 구축을 위한 ansible 입니다!!  
![제목을-입력해주세요_-002](https://github.com/user-attachments/assets/9bdd02de-d92a-43c5-bd2c-156e596face9)

- **[kafka, 애플리케이션 서버] 운영체제 : Ubuntu22.04**
- **https://github.com/sy0218/os_auto_ansible : (ubuntu 22.04) 운영체제 자동 setup ansible**
- **https://github.com/sy0218/ssh_keygen_auto_scripty : ssh-keygen 자동 스크립트**
- **https://github.com/sy0218/zookeeper_3.7.2_ansible : 주키퍼 자동 설치 및 동적 setup anible**
- **https://github.com/sy0218/kafka_3.6.2_auto_ansible : kafka 자동 설치 및 동적 setup anible**
- **https://github.com/sy0218/postgresql_12_ansible : postgresql 자동 설치 및 동적 setup anible**
- **https://github.com/sy0218/hadoop_3.3.5_ansible : 하둡 자동 설치 및 동적 setup anible**
- **https://github.com/sy0218/hive_3.1.3_ansible : hive 자동 설치 및 동적 setup anible**
- **https://github.com/sy0218/spark_3.4.3_ansible : 스파크 자동 설치 및 동적 setup anible**
- **https://github.com/sy0218/check : 운영에 유용한 스크립트를 모아놓은 레포지토리 입니다!!**  


<br><br>
---  
<br><br>

# 이상으로 매크로 탐지 시스템 프로젝트였습니다 감사합니다!!🙏🙏  

