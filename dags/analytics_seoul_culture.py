from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import psycopg2

RAW_SCHEMA = "raw_data"
ANALYTICS_SCHEMA = "analytics"
TEMP_TABLE = "temp_seoul_culture"
MAIN_TABLE = "seoul_culture"

KAKAO_API_KEY = Variable.get("kakao_api_key")
KAKAO_COORD_TO_REGION_URL = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"


# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn


# 쿼리문 실행 함수
def execute_query(query, parameters=None, autocommit=True, fetchall=False, executemany=False):
    conn = get_redshift_connection(autocommit)
    cur = conn.cursor()
    try:
        if not autocommit:
            cur.execute("BEGIN;")
        
        if parameters:
            if executemany:
                cur.executemany(query, parameters)
            else:
                cur.execute(query, parameters)
        else:
            cur.execute(query)
        
        if not autocommit:
            cur.execute("COMMIT;")
        
        if fetchall:
            return cur.fetchall()
    except psycopg2.DatabaseError as db_error:
        logging.error(f"Database error: {db_error}")
        cur.execute("ROLLBACK;")
        raise
    except Exception as general_error:
        logging.error(f"Exception error: {general_error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


# 좌표값을 이용해 시군구명, 법정동명을 받아오는 함수
def renamefetch_gu_and_dong(x, y):
    if not x or not y:
        return None, None
    
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"x": x, "y": y}
    try:
        response = requests.get(KAKAO_COORD_TO_REGION_URL, headers=headers, params=params)
        response.raise_for_status()  
        data = response.json()
        try:
            district_name = data["documents"][0].get("region_2depth_name")
            legal_dong_name = data["documents"][0].get("region_3depth_name")
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            logging.error(f"Data parsing error for coordinates ({x}, {y}): {e}")
            district_name, legal_dong_name = None, None
    except requests.RequestException as e:
        logging.error(f"API request error for coordinates ({x}, {y}): {e}")
        raise
    return district_name, legal_dong_name

# DAG 정의
@dag(
    dag_id="analytics_seoul_culture",
    start_date=datetime(2024, 8, 1),
    schedule_interval="0 13 * * 6",  # 매주 토요일 낮 1시
    catchup=False,
    tags=["analytics", "seoul", "culture", "infra"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def analytics_seoul_culture():
    # analytics 스키마에 테이블 생성 
    @task
    def create_temp_table():
        copy_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{TEMP_TABLE};

        CREATE TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE}(
            id INT IDENTITY(1, 1),
            culture_subject VARCHAR(100),
            name VARCHAR(100),
            address VARCHAR(200),
            latitude FLOAT DEFAULT NULL,
            longitude FLOAT DEFAULT NULL
        );
        """
        execute_query(copy_query, autocommit=False)

    # raw_data 스키마의 테이블에서 중복 제거 후 analytics에 삽입
    @task
    def insert_temp_table():
        insert_query = f"""
        INSERT INTO {ANALYTICS_SCHEMA}.{TEMP_TABLE} (address, culture_subject, name, latitude, longitude)
        SELECT DISTINCT address, subject_code, name, lat, lon
        FROM {RAW_SCHEMA}.{MAIN_TABLE}
        """
        execute_query(insert_query, autocommit=False)

    # 시군구 이름, 법정동 이름 컬럼 추가 
    @task
    def add_column_to_table():
        alter_query = f"""
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} ADD COLUMN district_name VARCHAR(30);
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} ADD COLUMN legal_dong_name VARCHAR(30);
        """
        execute_query(alter_query)

    # 시군구 이름, 법정동 이름 데이터 업데이트
    @task
    def update_district_and_dong():
        select_query = f"SELECT id, longitude, latitude FROM {ANALYTICS_SCHEMA}.{TEMP_TABLE}"
        
        rows = execute_query(select_query, fetchall=True)

        update_data = []
        for row in rows:
            id, x, y = row
            district_name, legal_dong_name = renamefetch_gu_and_dong(x, y)
            update_data.append((district_name, legal_dong_name, id))

        update_query = f"""
        UPDATE {ANALYTICS_SCHEMA}.{TEMP_TABLE} SET district_name = %s, legal_dong_name = %s
        WHERE id = %s
        """
        execute_query(update_query, parameters=update_data, executemany=True, autocommit=False)
    
    # 테이블 이름 변경 
    @task
    def rename_table():
        rename_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query, autocommit=False)

    create_temp_table() >> insert_temp_table() >> add_column_to_table() >> update_district_and_dong() >> rename_table()

analytics_seoul_culture()
