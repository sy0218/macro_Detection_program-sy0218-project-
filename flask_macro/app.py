#!/usr/bin/python3
from flask import Flask, render_template, request, jsonify
import subprocess
from postgres.postgres_hook import PostgresConnector

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/model_creation')
def model_creation():
    return render_template('model_creation.html')  # 모델 생성 페이지

@app.route('/run-script', methods=['POST'])
def run_script():
    data = request.get_json()  # JSON 형식으로 데이터 가져오기
    arg = data.get('arg')  # 인자 가져오기

    # arg가 None이면 에러 메시지 반환
    if arg is None:
        return "Error: 인자를 제공해야 합니다.", 400

    try:
        # 스크립트 실행
        subprocess.run(['docker', 'run', '--rm',
                        '-v', '/data/keyboard_sc/macro_model/model_materials:/app/model_materials',
                        'macro_dt_pg:0.0.1' ,'/app/model_materials', arg])
        return '모델 생성 완료!'
    except Exception as e:
        return f"Error executing script: {e}"

@app.route('/get-data', methods=['GET'])
def get_data():
    try:
        now_date = request.args.get('query')
        conn = PostgresConnector.get_connection('ps_20')  # 데이터베이스 연결
        cursor = conn.cursor()

        # 쿼리와 파라미터 출력
        print(f"Executing query: SELECT * FROM macro_pg_result WHERE pdate = {now_date} ORDER BY pred_result DESC, key_time_pc DESC")        

        cursor.execute("SELECT * FROM macro_pg_result where pdate = %s ORDER BY pred_result DESC, key_time_pc DESC", (now_date,))  # 사용자 쿼리 실행
        rows = cursor.fetchall()
        # 데이터가 있을 경우 반환
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        PostgresConnector.dis_connection()  # 연결 종료

@app.route('/get_consistency_data', methods=['GET'])
def get_consistency_data():
    try:
        # 쿼리 파라미터에서 날짜를 가져옵니다.
        consis_date = request.args.get('date')  # 'date'라는 키로 날짜를 받아옴
        if not consis_date:
            return jsonify({"error": "날짜를 입력해주세요."}), 400

        conn = PostgresConnector.get_connection('ps_20')
        cursor = conn.cursor()

        # 쿼리 출력
        print(f"Executing query: SELECT * FROM consistency_table WHERE pdate = {consis_date}")

        # 쿼리 실행
        cursor.execute("SELECT * FROM consistency_table WHERE pdate = %s", (consis_date,))
        rows = cursor.fetchall()

        # 데이터가 있을 경우 반환
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        PostgresConnector.dis_connection()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
