import datetime as dt
import base64
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

# 도커 오퍼레이터
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# dag 정의
dag = DAG(
    dag_id="keylog_airflow",
    start_date=dt.datetime(2024,10,13),
    end_date=dt.datetime(2024,10,14),
    schedule_interval="@daily"
    #start_date=dt.datetime.now(), # 현재 시각을 start_date로 설정
    #schedule_interval=None, # 테스트이기에 스케줄 인터벌 None로 설정 ( 나중에 매 00시로 변경 )
)

# TASK에 변수를 전달하기 위한 XCOM
def _keylog_xcom(**kwargs):
    xcom_key = kwargs['ti']
    execution_date = kwargs['execution_date']
    execution_date = kwargs['execution_date'] - dt.timedelta(days=1) # 하루 뺴기
    execution_date = execution_date.strftime('%Y%m%d') # 날짜를 원하는 형식으로 변환
    xcom_key.xcom_push(key='row_dir', value='/data/keyboard_sc/row_data/keylog')
    xcom_key.xcom_push(key='row_file', value=f"mp_keylog_{execution_date}.txt")
    xcom_key.xcom_push(key='batch_date', value=execution_date)
    xcom_key.xcom_push(key='hdfs_file_name', value=f"key_log_{execution_date}.parquet")
    xcom_key.xcom_push(key='hive_table_name', value='keylog_raw')
    xcom_key.xcom_push(key='backup_dir', value='/backup/keylog')
    xcom_key.xcom_push(key='object_version', value='v1')
push_xcom = PythonOperator(
    task_id='push_xcom',
    python_callable=_keylog_xcom,
    provide_context=True,
    dag=dag,
)

# keylog 원시데이터 모드 가져올떄까지 대기하는 Sensor
def _wait_for_rowdata(**kwargs):
    xcom_key = kwargs['ti']
    row_dir = xcom_key.xcom_pull(task_ids='push_xcom', key='row_dir')
    row_file = xcom_key.xcom_pull(task_ids='push_xcom', key='row_file')
    batch_date = xcom_key.xcom_pull(task_ids='push_xcom', key='batch_date')

    # 로그에 출력 확인
    print(f"Row Directory: {row_dir}")
    print(f"Batch Date: {batch_date}")
    
    keylog_row_path = Path(f"{row_dir}/{batch_date}")
    data_files = keylog_row_path / row_file
    success_file = keylog_row_path / "__SUCCESS__"

    # 센서 조건 지정
    if success_file.exists() and data_files:
        return True
    return False    

wait_keylog_rowdata = PythonSensor(
    task_id="wait_keylog_rowdata",
    python_callable=_wait_for_rowdata,
    mode="reschedule",
    dag=dag,
)

# 스파크 실행하는 SSHOperator
keylog_preprocesing = SSHOperator(
    task_id="preprocessing_use_spark",
    ssh_conn_id='apserver_ssh',
    command=(
        "spark-submit /data/keyboard_sc/pyspark_keylog/ch_data.py {{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }} {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }} {{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }} {{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}"
    ),
    cmd_timeout=360,
    dag=dag,
)

# hive 웨어하우스 적재하는 SSHOperator
keylog_to_hive = SSHOperator(
    task_id="to_hive",
    ssh_conn_id='apserver_ssh',
    command=(
        "bash /data/keyboard_sc/hive/hadoop_to_hive.sh "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }} "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }} "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='hive_table_name') }} "
    ),
    cmd_timeout=360,
    dag=dag,
)

# 원시 데이터, 전처리 데이터 gzip 압축 태스크
row_data_zip = SSHOperator(
    task_id="row_data_zip",
    ssh_conn_id='apserver_ssh',
    command=(
        "if [ -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz ]; then rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz; fi && "
        "gzip -c {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }} > /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz"
    ),
    cmd_timeout=360,
    dag=dag,
)
process_data_zip = SSHOperator(
    task_id="process_data_zip",
    ssh_conn_id='apserver_ssh',
    command=(
        "if [ -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} ]; then rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}; fi && "
        "hdfs dfs -get /user/hive/warehouse/{{ ti.xcom_pull(task_ids='push_xcom', key='hive_table_name') }}/pdate={{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} /tmp/ && "

        "if [ -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz ]; then rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz; fi && "
        "gzip /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}"
    ),
    cmd_timeout=360,
    dag=dag,
)


## 압축 파일 하둡 저장
row_zipfile_to_hdfs = SSHOperator(
    task_id="row_zipfile_to_hdfs",
    ssh_conn_id='apserver_ssh',
    command=(
        "if hdfs dfs -test -e {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/row_data/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz; then "
        "hdfs dfs -rm {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/row_data/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz; fi && "
        "hdfs dfs -put /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/row_data"
    ),
    cmd_timeout=360,
    dag=dag,
)
process_zipfile_to_hdfs = SSHOperator(
    task_id="process_zipfile_to_hdfs",
    ssh_conn_id='apserver_ssh',
    command=(
        "if hdfs dfs -test -e {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/process_data/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz; then "
        "hdfs dfs -rm {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/process_data/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz; fi && "
        "hdfs dfs -put /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/process_data"
    ),
    cmd_timeout=360,
    dag=dag,
)


## 압축 파일 aws s3 저장
row_zipfile_to_s3 = SSHOperator(
    task_id="row_zipfile_to_s3",
    ssh_conn_id='apserver_ssh',
    command=(
        "aws s3 cp /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz s3://sysytesttestbucket1/raw_data/"
    ),
    cmd_timeout=360,
    dag=dag,
)
process_zipfile_to_s3 = SSHOperator(
    task_id="process_zipfile_to_s3",
    ssh_conn_id='apserver_ssh',
    command=(
        "aws s3 cp /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz s3://sysytesttestbucket1/process_data/"
    ),
    cmd_timeout=360,
    dag=dag,
)


## 모든 백업 완료시 로컬 압축파일 삭제
delete_local_zipfile = SSHOperator(
    task_id="local_zipfile_all_remove",
    ssh_conn_id="apserver_ssh",
    command=(
        "rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz && "
        "rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz"
    ),
    cmd_timeout=360,
    dag=dag,
)


## [[최종]] 데이터 정합성 체크
count_rawdata = SSHOperator(
    task_id="raw_data_line_count",
    ssh_conn_id="apserver_ssh",
    command=(
        "wc -l < {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }} | tr -d '\\n'"
    ),
    cmd_timeout=360,
    do_xcom_push=True,
    dag=dag,
)
count_hivedata = SSHOperator(
    task_id="hive_data_line_count",
    ssh_conn_id="apserver_ssh",
    command=(
        "hive -e 'select count(*) from {{ ti.xcom_pull(task_ids='push_xcom', key='hive_table_name') }} where pdate='{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}'' | tr -d '\\n'"
    ),
    cmd_timeout=360,
    do_xcom_push=True,
    dag=dag,
)
count_raw_awk_data = SSHOperator(
    task_id="raw_data_awk_count",
    ssh_conn_id="apserver_ssh",
    command=(
        "cat {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }} | awk -F '[\\t,]' '{print NF}' | sort | uniq | tr -d '\\n'"
    ),
    cmd_timeout=360,
    do_xcom_push=True,
    dag=dag,
)


def _decode_functions(line_count, **kwargs):
    value = kwargs['ti'].xcom_pull(task_ids=line_count)
    decode_value = int(base64.b64decode(value).decode('utf-8').strip())
    return decode_value    
rawline_decode_task = PythonOperator(
    task_id='decode_raw_line',
    python_callable=_decode_functions,
    op_kwargs={'line_count': 'raw_data_line_count'},
    provide_context=True,
    dag=dag,
)
hiveline_decode_task = PythonOperator(
    task_id='decode_hive_line',
    python_callable=_decode_functions,
    op_kwargs={'line_count': 'hive_data_line_count'},
    provide_context=True,
    dag=dag,
)
rawawk_decode_task = PythonOperator(
    task_id='decode_raw_awk',
    python_callable=_decode_functions,
    op_kwargs={'line_count': 'raw_data_awk_count'},
    provide_context=True,
    dag=dag,
)


# 초기값 삽입
check_insert_task = PostgresOperator(
    task_id="check_and_insert_consistency",
    postgres_conn_id='apserver_postgres',
    sql="""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM consistency_table 
            WHERE pdate = TO_DATE('{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}', 'YYYYMMDD')
        ) THEN
            INSERT INTO consistency_table
            VALUES (0, 0, 0, TO_DATE('{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}', 'YYYYMMDD'));
        END IF;
    END $$;
    """,
    dag=dag,
)


# 업데이트 작업
update_rawline_task = PostgresOperator(
    task_id="update_consistency_raw",
    postgres_conn_id='apserver_postgres',
    sql="""
        UPDATE consistency_table
        SET raw_count = {{ task_instance.xcom_pull(task_ids='decode_raw_line') }}
        WHERE pdate = TO_DATE('{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}', 'YYYYMMDD');
    """,
    dag=dag,
)
update_hiveline_task = PostgresOperator(
    task_id="update_consistency_hive",
    postgres_conn_id='apserver_postgres',
    sql="""
        UPDATE consistency_table
        SET processed_count = {{ task_instance.xcom_pull(task_ids='decode_hive_line') }}
        WHERE pdate = TO_DATE('{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}', 'YYYYMMDD');
    """,
    dag=dag,
)
update_rawawk_task = PostgresOperator(
    task_id="update_consistency_rawawk",
    postgres_conn_id='apserver_postgres',
    sql="""
        UPDATE consistency_table
        SET awk_raw_count = {{ ti.xcom_pull(task_ids='decode_raw_awk') }}
        where pdate = TO_DATE('{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}', 'YYYYMMDD')
    """,
    dag=dag,
)


macro_detection_program = DockerOperator(
    task_id="detection_macro",
    image="exe_macro_dt_pg:1",
    command=[
        "python3",  
        "/app/run_macro_pg.py",
        "{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}",
        "{{ ti.xcom_pull(task_ids='push_xcom', key='object_version') }}"
    ],
    mounts=[
        Mount(
            source="/data/keyboard_sc/macro_model/model_materials",
            target="/app/model_materials",
            type="bind"
        )
    ],
    auto_remove=True, # 작업 완료 후 컨테이너 삭제
    dag=dag,
)

## 검증 완료시까지 체크 더미 오퍼레이터 정의
dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

## 보존정책에 따른 데이터 삭제 진행
local_data_remove = SSHOperator(
    task_id="rm_local_data",
    ssh_conn_id="apserver_ssh",
    command=(
        "bash /data/keyboard_sc/data_delete/local_data_rm.sh "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }} "
        "7 "
    ),
    cmd_timeout=360,
    dag=dag,
)
hive_data_remove = SSHOperator(
    task_id="rm_hive_data",
    ssh_conn_id="apserver_ssh",
    command=(
        "bash /data/keyboard_sc/data_delete/hive_warehouse_rm.sh "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }} "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='hive_table_name') }} "
        "7 "
        "pdate "
    ),
    cmd_timeout=360,
    dag=dag,
)
hdfs_data_remove = SSHOperator(
    task_id="rm_hdfs_data",
    ssh_conn_id="apserver_ssh",
    command=(
        "bash /data/keyboard_sc/data_delete/hdfs_data_rm.sh "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }} "
        "7 "
    ),
    cmd_timeout=360,
    dag=dag,
)
aws_s3_data_remove = SSHOperator(
    task_id="rm_aws_s3_data",
    ssh_conn_id="apserver_ssh",
    command=(
        "bash /data/keyboard_sc/data_delete/Aws_S3_rm.sh "
        "{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }} "
        "sysytesttestbucket1 "
        "10 "
    ),
    cmd_timeout=360,
    dag=dag,
)


# 의존성 정의
push_xcom >> wait_keylog_rowdata >> keylog_preprocesing >> keylog_to_hive
keylog_to_hive >> [row_data_zip, process_data_zip, macro_detection_program]
row_data_zip >> [row_zipfile_to_hdfs, row_zipfile_to_s3]
process_data_zip >> [process_zipfile_to_hdfs, process_zipfile_to_s3]
[row_zipfile_to_hdfs, row_zipfile_to_s3, process_zipfile_to_hdfs, process_zipfile_to_s3] >> delete_local_zipfile
delete_local_zipfile >> check_insert_task
check_insert_task >> count_rawdata >> rawline_decode_task >> update_rawline_task
check_insert_task >> count_hivedata >> hiveline_decode_task >> update_hiveline_task
check_insert_task >> count_raw_awk_data >> rawawk_decode_task >> update_rawawk_task
[update_rawline_task, update_hiveline_task, update_rawawk_task] >> dummy_task
dummy_task >> [local_data_remove, hive_data_remove, hdfs_data_remove, aws_s3_data_remove]
