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

# 주소 -> 좌표값 변환하는 API 호출 함수
def geocode(address):
    # 기본값 설정
    latitude = None
    longitude = None

    if not address:
        return latitude, longitude
    
    kakao_api_key = Variable.get('kakao_api_key')
    
    url = f'https://dapi.kakao.com/v2/local/search/address.json?query={address}&analyze_type=exact'
    headers = {"Authorization": kakao_api_key}
    
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        try:
            latitude = round(float(data['documents'][0]['y']), 6)
            longitude = round(float(data['documents'][0]['x']), 6)
        except (IndexError, KeyError, AttributeError, TypeError):
            pass  # 기본값 유지

    return latitude, longitude

# 위/경도 데이터 업데이트 Task
@task
def update_coordinates(table):
    cur = get_redshift_connection()
    # 위/경도 값이 비어있는 행만 처리
    cur.execute(f"SELECT id, road_address FROM analytics.temp_{table} WHERE latitude IS NULL OR longitude IS NULL")
    rows = cur.fetchall()

    for row in rows:
        id, address = row
        latitude, longitude = geocode(address)
        cur.execute(f"""
                UPDATE analytics.temp_{table} SET latitude = %s, longitude = %s
                WHERE id = %s
            """, (latitude, longitude, id))

# 컬럼 추가 Task
@task
def add_column_to_table(table):
    cur = get_redshift_connection()
    alter_query = f"""
    ALTER TABLE analytics.temp_{table} ADD COLUMN legal_dong_name VARCHAR(30);
    """
    cur.execute(alter_query)

# 좌표값 -> 지역명 변환하는 API 호출 함수
def fetch_dong_gu(x, y):
    kakao_api_key = Variable.get('kakao_api_key')
    url = f'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json?x={x}&y={y}'
    headers = {"Authorization": kakao_api_key}

    response = requests.get(url, headers=headers)
    dong = None  # 기본값 설정 
    if response.status_code == 200:
        data = response.json()
        try:
            dong = data['documents'][0]['region_3depth_name']
        except (IndexError, KeyError, AttributeError, TypeError):
            pass  # 기본값 유지

    return dong

# 동 이름 업데이트 Task
@task
def update_dong(table):
    cur = get_redshift_connection()
    
    cur.execute(f"SELECT id, longitude, latitude FROM analytics.temp_{table}")
    rows = cur.fetchall()

    for row in rows:
        id, x, y = row
        dong = fetch_dong_gu(x, y)
        
        cur.execute(f"""
                UPDATE analytics.temp_{table} SET legal_dong_name = %s
                WHERE id = %s
            """, (dong, id))

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
    dag_id='analytics_seoul_kindergarden',
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 20 * * 5',  # 한국 기준 토요일 새벽 5시
    catchup=False
) as dag:
    table = 'seoul_kindergarden'

    create_temp_table_task = create_temp_table(table)
    update_coordinates_task = update_coordinates(table)
    add_column_task = add_column_to_table(table)
    update_dong_task = update_dong(table)
    replace_table_task = replace_table(table)

    create_temp_table_task >> update_coordinates_task >> add_column_task >> update_dong_task >> replace_table_task
