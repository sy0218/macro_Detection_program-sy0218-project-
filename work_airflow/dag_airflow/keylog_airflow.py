import datetime as dt
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.ssh.operators.ssh import SSHOperator

# dag 정의
dag = DAG(
    dag_id="keylog_airflow",
    start_date=dt.datetime.now(), # 현재 시각을 start_date로 설정
    schedule_interval=None, # 테스트이기에 스케줄 인터벌 None로 설정 ( 나중에 매 00시로 변경 )
)

# TASK에 변수를 전달하기 위한 XCOM
def _keylog_xcom(**kwargs):
    xcom_key = kwargs['ti']
    execution_date = kwargs['execution_date']
    #execution_date = kwargs['execution_date'] - dt.timedelta(days=1) # 하루 뺴기
    execution_date = execution_date.strftime('%Y%m%d') # 날짜를 원하는 형식으로 변환
    xcom_key.xcom_push(key='row_dir', value='/data/keyboard_sc/row_data/keylog')
    xcom_key.xcom_push(key='batch_date', value=execution_date)
    xcom_key.xcom_push(key='hdfs_file_name', value='key_log.parquet')
    xcom_key.xcom_push(key='hive_table_name', value='keylog_raw')
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
    batch_date = xcom_key.xcom_pull(task_ids='push_xcom', key='batch_date')

    # 로그에 출력 확인
    print(f"Row Directory: {row_dir}")
    print(f"Batch Date: {batch_date}")
    
    keylog_row_path = Path(f"{row_dir}/{batch_date}")
    data_files = keylog_row_path / f"mp_keylog_{batch_date}.txt"
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
        "spark-submit /data/keyboard_sc/pyspark_keylog/ch_data.py mp_keylog_{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}.txt {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}"
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

# 의존성 정의
push_xcom >> wait_keylog_rowdata >> keylog_preprocesing >> keylog_to_hive
