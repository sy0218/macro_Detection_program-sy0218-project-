from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, udf
from pyspark.sql.types import StringType
import os
import subprocess
import sys

def _ppc_key_active(key_active_str):
    key_active_str = key_active_str.strip()
    key_active_str = key_active_str.replace('(','').replace(')','')
    return key_active_str

# UDF 등록
key_active_udf = udf(_ppc_key_active, StringType())


def main(file_name, file_dir, hdfs_file_name):
    # Spark 세션 생성
    spark = SparkSession.builder.appName("DataProcessing").master("local[*]").getOrCreate()
    # 파일 경로
    input_file = f"file://{file_dir}/{file_name}"

    # 데이터 읽기
    df = spark.read.text(input_file)

    # DataFrame 출력
    df.show(truncate=False)

    # 데이터 처리
    df = df.filter(df.value.contains("key event at") & df.value.contains("down"))
    df = df.withColumn('line', split(col('value'), ','))
    df = df.withColumn('key_log_time', split(col('line')[0], ' ')[3])
    df = df.withColumn('key_active', key_active_udf(split(col('line')[1], ' ')[2]))
    df = df.withColumn('user_id', split(col('line')[2], '\t')[1])
    # 필요한 열만 선택
    df = df.select('key_log_time','key_active','user_id')
    df.show(truncate=False)

    # 파일 저장 경로
    output_path = os.path.dirname(input_file.replace("file://", ""))

    # 멱등성 보장 ( 하둡 데이터 존재시 삭제 )
    check_cmd = subprocess.run(["hdfs", "dfs", "-test", "-e", f"{output_path}/*.parquet"], check=False)
    if check_cmd.returncode == 0:
        subprocess.run(["hdfs", "dfs", "-rm", f"{output_path}/*.parquet"], check=True)

    # Parquet 형식으로 저장
    df.write.mode("overwrite").parquet(output_path)

    # 스파크 세션 종료
    spark.stop()

    # hdfs에서 파일 이름 변경
    subprocess.run(["hdfs", "dfs", "-mv", f"{output_path}/*.parquet", f"{output_path}/{hdfs_file_name}"], check=True)

if __name__ == "__main__":
    args = sys.argv[1:]
    # 인자 두개 아니면 종료
    if len(args) != 3:
        print("사용법 : py <파일명> <파일경로> <저장 파일명>")
        sys.exit(1)

    file_name, file_dir, hdfs_file_name = args
    main(file_name, file_dir, hdfs_file_name)
