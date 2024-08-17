from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

SCHEMA = "raw_data"
TEMP_TABLE = "temp_seoul_academy"
MAIN_TABLE = "seoul_academy"

SEOUL_API_BASE_URL = "http://openapi.seoul.go.kr:8088/"
SEOUL_API_ENDPOINT = "json/neisAcademyInfo"
KAKAO_GEOCODE_URL = "https://dapi.kakao.com/v2/local/search/address.json"

SEOUL_API_KEY = Variable.get("seoul_api_key")
KAKAO_API_KEY = Variable.get("kakao_api_key")

# Redshift 연결 함수
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


# 쿼리 실행 함수
def execute_query(query, parameters=None, fetchall=False, executemany=False):
    conn = get_redshift_connection()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        if parameters:
            if executemany:
                cur.executemany(query, parameters)
            else:
                cur.execute(query, parameters)
        else:
            cur.execute(query)
        
        if fetchall:
            results = cur.fetchall()
        cur.execute("COMMIT;")
        if fetchall:
            return results
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error during query execution: {error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


# API 요청 응답 대한 전체 데이터의 개수를 반환하는 함수
def get_total_data_count():
    url = f"{SEOUL_API_BASE_URL}{SEOUL_API_KEY}/{SEOUL_API_ENDPOINT}/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data["neisAcademyInfo"]["list_total_count"]
    except requests.RequestException as error:
        logging.error(f"API request failed: {error}")
        raise


# 학원 데이터를 API에서 가져오는 함수
def fetch_data(start_index, end_index):
    url = f"{SEOUL_API_BASE_URL}{SEOUL_API_KEY}/{SEOUL_API_ENDPOINT}/{start_index}/{end_index}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("neisAcademyInfo", {}).get("row", [])
    except requests.RequestException as error:
        logging.error(f"API request failed for range {start_index}-{end_index}: {error}")
        raise


# 데이터를 변환하는 함수
def transform_data(data_list):
    records = []
    for data in data_list:
        record = (
            data.get("ADMST_ZONE_NM"),
            data.get("ACA_ASNUM"),
            data.get("ACA_NM"),
            int(data.get("TOFOR_SMTOT", 0)),
            data.get("REALM_SC_NM"),
            data.get("LE_CRSE_NM"),
            data.get("FA_RDNZC"),
            data.get("FA_RDNMA"),
            data.get("FA_RDNDA")
        )
        records.append(record)

    return records


# 주소 데이터로 좌표 데이터와 법정동명 받아오는 함수
def geocode(address):
    params = {"query": address}
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    try:
        response = requests.get(KAKAO_GEOCODE_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json().get("documents", [])
        try:
            latitude = float(data[0]["y"])
            longitude = float(data[0]["x"])
            legal_dong_name = data[0].get("road_address", {}).get("region_3depth_name", None)
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            logging.warning(f"Error processing geocode response address {address}: {e}")
            latitude = None
            longitude = None
            legal_dong_name = None
    except requests.RequestException as e:
        logging.error(f"Failed to geocode address {address}: {e}")
        raise

    return latitude, longitude, legal_dong_name


@dag(
    start_date=datetime(2024, 7, 10),
    schedule_interval="50 9 * * 6",  
    catchup=False,
    tags=["academy", "raw_data", "infra", "weekly"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def seoul_academy():

    # 테이블 생성
    @task
    def create_temp_table():
        create_query = f"""
        DROP TABLE IF EXISTS {SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            district_name VARCHAR(50),
            academy_code VARCHAR(50),
            academy_name VARCHAR(100),
            total_capacity INT,
            category_name VARCHAR(100),
            course_name VARCHAR(100),
            postal_code VARCHAR(6),
            road_address VARCHAR(255),
            detailed_road_address VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            legal_dong_name VARCHAR(30)
        );
        """
        execute_query(create_query)


    # 데이터 수집 및 변형 
    @task
    def process_data():
        start_index = 1
        unit_value = 1000
        total_count = get_total_data_count()
        all_records = []
        
        while start_index <= total_count:
            end_index = min(start_index + unit_value - 1, total_count)
            data_list = fetch_data(start_index, end_index)
            records = transform_data(data_list)
            all_records.extend(records)
            start_index += unit_value
        
        return all_records
        

    # 위/경도 좌표값 추가
    @task
    def add_coordinate(records):
        geocoded_records = []

        for record in records:
            road_address = record[7]  # 주소 FA_RDNMA
            if not road_address:
                geocoded_records.append(list(record) + [None, None, None])
                continue

            latitude, longitude, legal_dong_name = geocode(road_address)
            geocoded_records.append(tuple(list(record) + [latitude, longitude, legal_dong_name]))

        return geocoded_records


    # 데이터 적재 
    @task
    def load_temp_table(records):
        insert_query = f"""
        INSERT INTO {SCHEMA}.{TEMP_TABLE} (
            district_name, academy_code, academy_name, 
            total_capacity, category_name, course_name,
            postal_code, road_address, detailed_road_address,
            latitude, longitude, legal_dong_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, parameters=records, executemany=True)

    # 테이블 이름 변경 
    @task
    def rename_tables():
        rename_query = f"""
        DROP TABLE IF EXISTS {SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query)

    create_temp_table_task = create_temp_table()
    process_data_task = process_data()
    geocode_task = add_coordinate(process_data_task)
    load_temp_table_task = load_temp_table(geocode_task)
    rename_tables_task = rename_tables()

    create_temp_table_task >> process_data_task >> geocode_task >> load_temp_table_task >> rename_tables_task

seoul_academy()