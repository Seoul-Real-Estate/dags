from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

RAW_SCHEMA = "raw_data"
TEMP_TABLE = "temp_seoul_park"
MAIN_TABLE = "seoul_park"

SEOUL_API_KEY = Variable.get("seoul_api_key")
SEOUL_API_BASE_URL = "http://openapi.seoul.go.kr:8088"
SEOUL_API_RESPONSE_TYPE = "json"
SEOUL_API_ENDPOINT = "SearchParkInfoService"

KAKAO_COORD_TO_REGION_URL = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
KAKAO_API_KEY = Variable.get("kakao_api_key")

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn


# 쿼리문 실행시키는 함수
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


# 전체 데이터의 개수를 반환하는 함수
def get_list_total_count():
    url = f"{SEOUL_API_BASE_URL}/{SEOUL_API_KEY}/{SEOUL_API_RESPONSE_TYPE}/{SEOUL_API_ENDPOINT}/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data[SEOUL_API_ENDPOINT]["list_total_count"]
    except requests.RequestException as error:
        logging.error(f"API request failed: {error}")
        raise


# 영화상영관 데이터를 API에서 가져오는 함수
def fetch_seoul_data(start_index, end_index):
    url = f"{SEOUL_API_BASE_URL}/{SEOUL_API_KEY}/{SEOUL_API_RESPONSE_TYPE}/{SEOUL_API_ENDPOINT}/{start_index}/{end_index}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()[SEOUL_API_ENDPOINT]["row"]
    except Exception as error:
        logging.error(f"failed fetch seoul data: {error}")
        raise


# 데이터 변환
def transform_data(data_list):
    records = []
    for data in data_list:
        record = (
            data["P_PARK"],
            data["P_ZONE"],
            data["P_ADDR"],
            data["LATITUDE"],
            data["LONGITUDE"]
        )
        records.append(record)
    return records


# 좌표로 법정동 정보를 가져오는 함수
def get_dong_name(x, y):
    if not x or not y:
        return None
    
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"x": x, "y": y}
    try:
        response = requests.get(KAKAO_COORD_TO_REGION_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        legal_dong_name = data["documents"][0]["region_3depth_name"]
    except (IndexError, KeyError, TypeError, AttributeError) as e:
        logging.error(f"Error processing dong name response for coordinates ({x}, {y}): {e}")
        legal_dong_name = None
    except Exception as e:
        logging.error(f"Failed to get dong name for coordinates ({x}, {y}): {e}")
        raise
    return legal_dong_name

# DAG 정의
@dag(
    start_date=datetime(2024, 7, 10),
    schedule_interval="0 1 * * 6",  # 한국 기준 토요일 오전 1시
    catchup=False,
    tags=["infra", "park", "raw_data"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "kain",
    }
)
def seoul_park():
    # 테이블 생성 task
    @task
    def create_temp_table():
        create_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {RAW_SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            park_name VARCHAR(100),
            district_name VARCHAR(50),
            address VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            legal_dong_name VARCHAR(50)
        );
        """
        execute_query(create_query)


    # 데이터 수집 task
    @task
    def process_data():
        total_count = get_list_total_count()
        start_index = 1
        unit_value = 1000
        all_records = []

        while start_index <= total_count:
            end_index = min(start_index + unit_value - 1, total_count)
            data_list = fetch_seoul_data(start_index, end_index)
            records = transform_data(data_list)
            all_records.extend(records)
            start_index += unit_value
        logging.info(f"process_raw_data {len(all_records)} records.")

        return all_records
    

    # 법정동 데이터 추가하는 task
    @task
    def add_dong_data(records):
        geocoded_records = []
        for record in records:
            latitude, longitude = record[3], record[4]

            legal_dong_name = get_dong_name(longitude, latitude)
            
            geocoded_record = list(record) + [legal_dong_name]
            geocoded_records.append(tuple(geocoded_record))
        logging.info(f"Geocoded {len(geocoded_records)} records.")
        return geocoded_records
    

    # 테이블에 데이터 적재하는 task
    @task
    def load_temp_table(records):
        insert_query = f"""
        INSERT INTO {RAW_SCHEMA}.{TEMP_TABLE} (
            park_name, district_name, address, latitude, longitude, legal_dong_name
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, parameters=records, executemany=True, autocommit=False)


    # 테이블 이름 변경하는 task
    @task
    def rename_tables():
        rename_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {RAW_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query, autocommit=False)

    create_temp_table_task = create_temp_table()
    process_data_task = process_data()
    add_dong_data_task = add_dong_data(process_data_task)
    load_temp_table_task = load_temp_table(add_dong_data_task)
    rename_tables_task = rename_tables()

    create_temp_table_task >> process_data_task >> add_dong_data_task >> load_temp_table_task >> rename_tables_task

seoul_park()
