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
    xcom_key.xcom_push(key='row_file', value=f"mp_keylog_{execution_date}.txt")
    xcom_key.xcom_push(key='batch_date', value=execution_date)
    xcom_key.xcom_push(key='hdfs_file_name', value=f"key_log_{execution_date}.parquet")
    xcom_key.xcom_push(key='hive_table_name', value='keylog_raw')
    xcom_key.xcom_push(key='backup_dir', value='/backup/keylog')
push_xcom = PythonOperator(
    task_id='push_xcom',
    python_callable=_keylog_xcom,
    provide_context=True,
    dag=dag,
)

# keylog 원시데이터 모드 가져올떄까지 대기하는 Sensor
#def _wait_for_rowdata(**kwargs):
#    xcom_key = kwargs['ti']
#    row_dir = xcom_key.xcom_pull(task_ids='push_xcom', key='row_dir')
#    row_file = xcom_key.xcom_pull(task_ids='push_xcom', key='row_file')
#    batch_date = xcom_key.xcom_pull(task_ids='push_xcom', key='batch_date')
#
#    # 로그에 출력 확인
#    print(f"Row Directory: {row_dir}")
#    print(f"Batch Date: {batch_date}")
#    
#    keylog_row_path = Path(f"{row_dir}/{batch_date}")
#    data_files = keylog_row_path / row_file
#    success_file = keylog_row_path / "__SUCCESS__"
#
#    # 센서 조건 지정
#    if success_file.exists() and data_files:
#        return True
#    return False    
#
#wait_keylog_rowdata = PythonSensor(
#    task_id="wait_keylog_rowdata",
#    python_callable=_wait_for_rowdata,
#    mode="reschedule",
#    dag=dag,
#)
#
## 스파크 실행하는 SSHOperator
#keylog_preprocesing = SSHOperator(
#    task_id="preprocessing_use_spark",
#    ssh_conn_id='apserver_ssh',
#    command=(
#        "spark-submit /data/keyboard_sc/pyspark_keylog/ch_data.py {{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }} {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }} {{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}"
#    ),
#    cmd_timeout=360,
#    dag=dag,
#)
#
## hive 웨어하우스 적재하는 SSHOperator
#keylog_to_hive = SSHOperator(
#    task_id="to_hive",
#    ssh_conn_id='apserver_ssh',
#    command=(
#        "bash /data/keyboard_sc/hive/hadoop_to_hive.sh "
#        "{{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }} "
#        "{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }} "
#        "{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} "
#        "{{ ti.xcom_pull(task_ids='push_xcom', key='hive_table_name') }} "
#    ),
#    cmd_timeout=360,
#    dag=dag,
#)

## 원시 데이터, 전처리 데이터 gzip 압축
backup_data_zip = SSHOperator(
    task_id="data_zip",
    ssh_conn_id='apserver_ssh',
    command=(
        # 멱등성을 위한 삭제후 > 압축
        "if [ -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} ]; then rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}; fi && "
        "hdfs dfs -get /user/hive/warehouse/{{ ti.xcom_pull(task_ids='push_xcom', key='hive_table_name') }}/pdate={{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} /tmp/ && "
 
        "if [ -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz ]; then rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz; fi && "
        "gzip /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }} && "

        "if [ -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz ]; then rm -f /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz; fi && "
        "gzip -c {{ ti.xcom_pull(task_ids='push_xcom', key='row_dir') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='batch_date') }}/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }} > /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz"
    ),
    cmd_timeout=360,
    dag=dag,
)

# 압축 파일 하둡 저장
zipfile_to_hdfs = SSHOperator(
    task_id="zip_to_hdfs",
    ssh_conn_id='apserver_ssh',
    command=(
    # 멱등성 위해 삭제후 저장
    "if hdfs dfs -test -e {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/row_data/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz; then "
    "hdfs dfs -rm {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/row_data/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz; "
    "fi && "

    "if hdfs dfs -test -e {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/process_data/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz; then "
    "hdfs dfs -rm {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/process_data/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz; "
    "fi && "

    "hdfs dfs -put /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='row_file') }}.gz {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/row_data && "
    "hdfs dfs -put /tmp/{{ ti.xcom_pull(task_ids='push_xcom', key='hdfs_file_name') }}.gz {{ ti.xcom_pull(task_ids='push_xcom', key='backup_dir') }}/process_data"
    ),
    cmd_timeout=360,
    dag=dag,
)

# 의존성 정의
#push_xcom >> wait_keylog_rowdata >> keylog_preprocesing >> keylog_to_hive
push_xcom >> backup_data_zip >> zipfile_to_hdfs
