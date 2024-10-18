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
