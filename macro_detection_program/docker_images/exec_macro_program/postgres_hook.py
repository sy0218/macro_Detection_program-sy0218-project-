#!/usr/bin/python3
import psycopg2

class PostgresConnector:
    mt_conn = None # 연결 객체 저장
    
    @classmethod
    def get_connection(cls, postgres_type=None):
        ps_type_dt = {'ps_10':['sy0218', 'sy0218', 'sy0218', '192.168.56.10', '5432'],
                      'ps_20': ['sy0218', 'sy0218', 'sy0218', '192.168.219.20', '5432']}
        if postgres_type == None:
            raise ValueError("인자가 부족합니다: postgres_type 를 제공해야 합니다.")
        if postgres_type not in ps_type_dt:
            raise ValueError(f"존재하지 않는 psotgres_type입니다: {postgres_type}")
        
        dbname, user, password, host, port = ps_type_dt[postgres_type]
        if cls.mt_conn is None:
            try:
                cls.mt_conn = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )
                print(f"{host} postgres {dbname} connection successful.")
            except Exception as e:
                print(f"postgres connecting error {e}")
        return cls.mt_conn

    @classmethod
    def dis_connection(cls):
        if cls.mt_conn is not None:
            cls.mt_conn.close() # 연결 종료
            print("postgres disconnecting successful")
            cls.mt_conn = None
