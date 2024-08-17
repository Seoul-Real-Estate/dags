from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import psycopg2

RAW_SCHEMA = "raw_data"
ANALYTICS_SCHEMA = "analytics"
MAIN_TABLE = "seoul_hospital"
TEMP_TABLE = "temp_seoul_hospital"

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
    except Exception as error:
        logging.error(f"execute query error: {error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


# 지역구/법정동명 가져오는 함수
def fetch_gu_dong(x, y):
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"x": x, "y": y}
    try:
        response = requests.get(KAKAO_COORD_TO_REGION_URL, headers=headers, params=params)
        
        if response.status_code == 400:
            logging.warning(f"Bad Request for coordinates ({x}, {y}): {response.text}")
            return None, None
        
        response.raise_for_status()
        data = response.json()
        try:
            district_name = data["documents"][0]["region_2depth_name"]
            legal_dong_name = data["documents"][0]["region_3depth_name"]
        except (IndexError, KeyError, AttributeError, TypeError, response) as error:
            logging.warning(f"Failed to get gu, dong for coordinates ({x}, {y}): {error}")
            district_name, legal_dong_name = None, None
    except Exception as e:
        logging.error(f"API request failed: {e}")
        raise

    return district_name, legal_dong_name
    

# DAG 정의
@dag(
    start_date=datetime(2024, 8, 1),
    schedule_interval="0 13 28 * *",  # 매월 28일 오후 1시
    catchup=False,
    tags=["hospital", "analytics", "infra"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def analytics_seoul_hospital():

    # 임시 테이블 생성 Task
    @task
    def create_temp_table():
        copy_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} AS
        SELECT DISTINCT * FROM {RAW_SCHEMA}.{MAIN_TABLE};
        """
        execute_query(copy_query)


    # 컬럼 추가 Task
    @task
    def add_columns():
        alter_query = f"""
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} ADD COLUMN district_name VARCHAR(30);
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} ADD COLUMN legal_dong_name VARCHAR(30);
        """
        execute_query(alter_query)


    # 구 이름과 동 이름 업데이트 Task
    @task
    def update_gu_dong():
        select_query = f"SELECT id, lon, lat FROM {ANALYTICS_SCHEMA}.{TEMP_TABLE}"
        rows = execute_query(select_query, fetchall=True)
        update_data = []

        for row in rows:
            id, x, y = row
            district_name, legal_dong_name = fetch_gu_dong(x, y)
            update_data.append((district_name, legal_dong_name, id))
            
        update_query = f"""
        UPDATE {ANALYTICS_SCHEMA}.{TEMP_TABLE} 
        SET district_name = %s, legal_dong_name = %s
        WHERE id = %s
        """
        execute_query(update_query, parameters=update_data, executemany=True, autocommit=False)


    # 테이블 교체 Task
    @task
    def rename_table():
        rename_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query, autocommit=False)

    create_temp_table() >> add_columns() >> update_gu_dong() >> rename_table()

analytics_seoul_hospital()
