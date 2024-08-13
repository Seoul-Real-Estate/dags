from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

SCHEMA_RAW = "raw_data"
SCHEMA_ANALYTICS = "analytics"
TEMP_TABLE = "temp_seoul_park"
MAIN_TABLE = "seoul_park"

SEOUL_API_KEY = Variable.get("seoul_api_key")
SEOUL_API_BASE_URL = "http://openapi.seoul.go.kr:8088"
SEOUL_API_RESPONSE_TYPE = "json/"
SEOUL_API_ENDPOINT = "SearchParkInfoService"

API_KAKAO_URL = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
API_KAKAO_KEY = Variable.get("kakao_api_key")

# Redshift 연결 함수
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    return conn


# 쿼리 실행 함수
def execute_query(query, parameters=None, fetchall=False, executemany=False, autocommit=True):
    conn = get_redshift_connection()
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
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Query error: {error}")
        if not autocommit:
            cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


# 전체 데이터의 개수를 반환하는 함수
def get_list_total_count():
    url = f"{SEOUL_API_BASE_URL}{SEOUL_API_KEY}/{SEOUL_API_ENDPOINT}/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data[SEOUL_API_ENDPOINT]["list_total_count"]
    except requests.RequestException as error:
        logging.error(f"API request failed: {error}")
        raise


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
        DROP TABLE IF EXISTS {SCHEMA_ANALYTICS}.{TEMP_TABLE};
        CREATE TABLE {SCHEMA_ANALYTICS}.{TEMP_TABLE} (
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
        start_index = 1
        unit_value = 1000
        records = []

        total_count = get_list_total_count()

        while start_index <= total_count:
            end_index = min(start_index + unit_value - 1, total_count)
            url = f"{SEOUL_API_BASE_URL}{SEOUL_API_KEY}/{SEOUL_API_ENDPOINT}/{start_index}/{end_index}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data_list = response.json()[SEOUL_API_ENDPOINT]["row"]

                for data in data_list:
                    record = (
                        data["P_PARK"],
                        data["P_ZONE"],
                        data["P_ADDR"],
                        data["LATITUDE"],
                        data["LONGITUDE"]
                    )
                    records.append(record)

                start_index += unit_value
            except Exception as error:
                logging.error(f"process data failed: {error}")
                raise

        return records


    # 법정동 데이터 추가하는 task
    @task
    def add_dong_data(records):
        geocoded_records = []
        url_template = API_KAKAO_URL.format
        headers = {"Authorization": f"KakaoAK {API_KAKAO_KEY}"}

        for record in records:
            latitude, longitude = record[3], record[4]
            url = url_template(longitude=longitude, latitude=latitude)

            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                dong = data["documents"][0].get("region_3depth_name", None)
            except (IndexError, KeyError, TypeError, AttributeError) as error:
                logging.error(f"Failed to get dong name for coordinates ({longitude}, {latitude}): {error}")
                dong = None
            except Exception as error:
                logging.error(f"API request faile : {error}")
                raise

            geocoded_record = list(record) + [dong]
            geocoded_records.append(tuple(geocoded_record))

        return geocoded_records

    # 테이블에 데이터 적재하는 task
    @task
    def load_temp_table(records):
        insert_query = f"""
        INSERT INTO {SCHEMA_ANALYTICS}.{TEMP_TABLE} (
            park_name, district_name, address, latitude, longitude, legal_dong_name
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, parameters=records, executemany=True, autocommit=False)

    # 테이블 이름 변경하는 task
    @task
    def rename_tables():
        rename_query = f"""
        DROP TABLE IF EXISTS {SCHEMA_ANALYTICS}.{MAIN_TABLE};
        ALTER TABLE {SCHEMA_ANALYTICS}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query, autocommit=False)

    create_temp_table() >> process_data() >> add_dong_data() >> load_temp_table() >> rename_tables()

seoul_park()
