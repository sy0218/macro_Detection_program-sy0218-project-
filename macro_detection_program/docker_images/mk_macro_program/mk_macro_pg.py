#!/usr/bin/python3
from postgres.postgres_hook import PostgresConnector
import sys

import psycopg2
from psycopg2 import sql

import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Embedding
from tensorflow.keras.preprocessing.sequence import pad_sequences

# 과적합 방지를 위해 얼리 스탑핑
from tensorflow.keras.callbacks import EarlyStopping

def main(materials_path, batch_date):
    query = "SELECT * FROM keylog_modeling_data"
    postgres_conn = PostgresConnector.get_connection('ps_20')
    df = pd.read_sql_query(query, postgres_conn) # postgres 연결
    PostgresConnector.dis_connection() # postgres 종료

    # 사용자별 키로그 입력 시간 오름차순 정렬
    df = df[['key_log_time', 'key_active', 'user_id','label']].sort_values(by=['user_id', 'key_log_time'], ascending=[True, True])
    df = df.groupby('user_id').agg({
        'key_active': lambda x: list(x),
        'label': 'first'
    }).reset_index()

    # lstm 모델링을 위한 전처리 레고레고
    # 시퀀스 변환, 패딩
    max_length = 50
    tokenizer = {key: i+1 for i, key in enumerate(set(np.concatenate(df['key_active'].values)))}
    X = [[tokenizer[key] for key in seq] for seq in df['key_active']]
    y = df['label'].values
    X = pad_sequences(X, maxlen=max_length, padding='post')

    # 데이터 분할
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # LSTM 모델링
    model = Sequential()
    # len(tokenizer)+1 개의 토근을 8차원 백터로 변환후, 각 시퀀스 길이는 30으로 고정
    model.add(Embedding(input_dim=len(tokenizer)+1, output_dim=8, input_length=max_length))
    model.add(LSTM(32))
    model.add(Dense(1, activation='sigmoid'))

    model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])

    # 과적합 방지 위한 얼리 스탑핑 ( 검증 손실이 3번 연속 개선안되면 학습을 중지!! )
    early_stopping = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)

    # 모델 학습
    model.fit(X_train, y_train, epochs=30, batch_size=10, validation_data=(X_test, y_test), callbacks=[early_stopping])

    # 모델 평가
    loss, accuracy = model.evaluate(X_test, y_test)
    print(f'Loss: {loss}, Accuracy: {accuracy}')

    # 테스트 데이터 예측
    pred = model.predict(X_test)
    print(pred, y_test)

    # 모델 및 토큰 객체 저장
    model_filename = f"macro_model_{batch_date}.keras"
    tokenizer_filename = f"macro_tokenizer_{batch_date}.pkl"
    model.save(f"{materials_path}/{model_filename}")
    with open(f'{materials_path}/{tokenizer_filename}', 'wb') as f:
        pickle.dump(tokenizer, f)

if __name__ == "__main__":
    args = sys.argv[1:]
    # 인자 두개 아니면 종료
    if len(args) != 2:
        print("사용법 : py <객체경로> <저장 버전명>")
        sys.exit(1)

    materials_path, batch_date = args
    main(materials_path, batch_date)
