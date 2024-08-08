from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import requests

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 테이블 복제 Task
@task
def create_temp_table(table):
    cur = get_redshift_connection()
    copy_query = f"""
    DROP TABLE IF EXISTS analytics.temp_{table};

    CREATE TABLE analytics.temp_{table} AS
    SELECT DISTINCT * FROM raw_data.{table};
    """
    cur.execute(copy_query)

# 컬럼 추가 task
@task
def add_column_to_table(table):
    cur = get_redshift_connection()
    alter_query = f"""
    ALTER TABLE analytics.temp_{table} ADD COLUMN district_name VARCHAR(30);
    ALTER TABLE analytics.temp_{table} ADD COLUMN legal_dong_name VARCHAR(30);
    """
    cur.execute(alter_query)

# API 호출 함수
def fetch_gu_dong(x, y):
    kakao_api_key = Variable.get('kakao_api_key')
    url = f'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json?x={x}&y={y}'
    headers = {"Authorization": kakao_api_key}

    response = requests.get(url, headers=headers)
    # 기본값 설정
    gu = None
    dong = None
    if response.status_code == 200:
        data = response.json()
        try:
            gu = data['documents'][0]['region_2depth_name']
            dong = data['documents'][0]['region_3depth_name']
        except (IndexError, KeyError, AttributeError, TypeError):
            pass  # 기본값 유지

    return gu, dong

# 구 이름과 동 이름과 업데이트 Task
@task
def update_gu_dong(table):
    cur = get_redshift_connection()
    
    # 데이터 선택
    cur.execute(f"SELECT number, lon, lat FROM analytics.temp_{table}")
    rows = cur.fetchall()

    for row in rows:
        id, x, y = row
        gu, dong = fetch_gu_dong(x, y)
        
        cur.execute(f"""
                UPDATE analytics.temp_{table} SET district_name = %s, legal_dong_name = %s
                WHERE number = %s
            """, (gu, dong, id))

# 테이블 교체 Task
@task
def replace_table(table):
    cur = get_redshift_connection(False)
    cur.execute("BEGIN;")
    replace_query = f"""
    DROP TABLE IF EXISTS analytics.{table};

    ALTER TABLE analytics.temp_{table} RENAME TO {table};
    """
    cur.execute(replace_query)
    cur.execute("COMMIT;")

# DAG 정의
with DAG(
    dag_id='analytics_seoul_culture',
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 4 * * 6',  # 한국 기준 매주 토요일 낮 1시
    catchup=False
) as dag:
    table = 'seoul_culture'

    create_temp_table_task = create_temp_table(table)
    add_column_task = add_column_to_table(table)
    update_gu_dong_task = update_gu_dong(table)
    replace_table_task = replace_table(table)

    create_temp_table_task >> add_column_task >> update_gu_dong_task >> replace_table_task
