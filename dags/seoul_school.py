from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

RAW_SCHEMA = "raw_data"
ANALYTICS_SCHEMA = "analytics"
TEMP_TABLE = "temp_seoul_school"
MAIN_TABLE = "seoul_school"

SEOUL_API_KEY = Variable.get("seoul_api_key")
BASE_URL = "http://openapi.seoul.go.kr:8088/"
ENDPOINT = "/json/neisSchoolInfo"

KAKAO_API_KEY = Variable.get("kakao_api_key")
KAKAO_GEOCODE_URL = "https://dapi.kakao.com/v2/local/search/address.json"


# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn


# 쿼리 실행 함수
def execute_query(query, parameters=None, fetchall=False, executemany=False):
    conn = get_redshift_connection(autocommit=False)
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
    except (psycopg2.DatabaseError, Exception) as error:
        logging.error(f"Error during query execution: {error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


# 전체 데이터 개수를 반환하는 함수
def get_total_data_count():
    url = f"{BASE_URL}{SEOUL_API_KEY}{ENDPOINT}/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data["neisSchoolInfo"]["list_total_count"]
    except Exception as error:
        logging.error(f"API request failed: {error}")
        raise


# 학교 데이터를 API에서 가져오는 함수
def fetch_data(start_index, end_index):
    url = f"{BASE_URL}{SEOUL_API_KEY}{ENDPOINT}/{start_index}/{end_index}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("neisSchoolInfo", {}).get("row", [])
    except Exception as error:
        logging.error(f"API request failed for range {start_index}-{end_index}: {error}")
        raise


# 데이터를 변환하는 함수
def transform_data(data_list):
    records = []
    for data in data_list:
        record = (
            data["SD_SCHUL_CODE"],
            data["SCHUL_NM"],
            data["SCHUL_KND_SC_NM"],
            data["ORG_RDNZC"],
            data["ORG_RDNMA"],
            data["ORG_RDNDA"],
            data["LOAD_DTM"]
        )
        records.append(record)

    return records


# 주소로 좌표 정보를 가져오는 함수
def geocode_address(address):
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address}
    try:
        response = requests.get(KAKAO_GEOCODE_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        try:
            latitude = float(data["documents"][0]["y"])
            longitude = float(data["documents"][0]["x"])
            district_name = data["documents"][0]["road_address"]["region_2depth_name"]  
            legal_dong_name = data["documents"][0]["road_address"]["region_3depth_name"]  
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            logging.error(f"Error processing geocode response address {address}: {e}")
            latitude, longitude, district_name, legal_dong_name = None, None, None, None
    except Exception as e:
        logging.error(f"Failed to geocode address {address}: {e}")
        raise
    return latitude, longitude, district_name, legal_dong_name


# DAG 정의
@dag(
    start_date=datetime(2024, 7, 10),
    schedule_interval="0 10 * * 6",  # 토요일 오전 10시 
    catchup=False,
    tags=["analytics", "weekly", "infra", "raw_data", "school"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def seoul_school():

    # RAW 스키마에 테이블 생성
    @task
    def create_raw_table():
        create_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {RAW_SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            school_code VARCHAR(7),
            school_name VARCHAR(100),
            school_type VARCHAR(50),
            postal_code VARCHAR(6),
            road_address VARCHAR(150),
            detailed_road_address VARCHAR(150),
            load_date VARCHAR(8)
        );
        """
        execute_query(create_query)


    # 데이터 수집 
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


    # 데이터 적재
    @task
    def load_raw_table(records):
        insert_query = f"""
        INSERT INTO {RAW_SCHEMA}.{TEMP_TABLE} (
            school_code, school_name, school_type, postal_code, 
            road_address, detailed_road_address, load_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, parameters=records, executemany=True)


    # 테이블 이름 변경 
    @task
    def rename_raw_table():
        rename_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {RAW_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query)


    # ANALYTICS 스키마에 테이블 생성
    @task
    def create_analytics_table():
        create_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{TEMP_TABLE};

        CREATE TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1),
            school_code VARCHAR(7),
            school_name VARCHAR(100),
            school_type VARCHAR(50),
            postal_code VARCHAR(6),
            road_address VARCHAR(150),
            detailed_road_address VARCHAR(150),
            load_date VARCHAR(8),
            district_name VARCHAR(30),
            legal_dong_name VARCHAR(30),
            latitude float,
            longitude float
        );
        """
        execute_query(create_query)


    # ANALYTICS 스키마에 데이터 삽입
    @task
    def insert_geocoded_school_data():
        # raw_data의 중복 데이터 제거 
        select_query = f"""
        SELECT DISTINCT school_code, school_name, school_type, postal_code, 
                        road_address, detailed_road_address, load_date
        FROM {RAW_SCHEMA}.{MAIN_TABLE}
        """

        rows = execute_query(select_query, fetchall=True)
        analytics_records = []

        for row in rows:
            school_code, school_name, school_type, postal_code, road_address, detailed_road_address, load_date = row

            # 위/경도 좌표값, 구/동 이름 추가
            latitude, longitude, district_name, legal_dong_name = geocode_address(road_address)

            analytics_record = (
                school_code, school_name, school_type, postal_code, 
                road_address, detailed_road_address, load_date, 
                district_name, legal_dong_name, latitude, longitude
            )
            analytics_records.append(analytics_record)

        insert_query = f"""
        INSERT INTO {ANALYTICS_SCHEMA}.{TEMP_TABLE} (
            school_code, school_name, school_type, postal_code, 
            road_address, detailed_road_address, load_date, 
            district_name, legal_dong_name, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, parameters=analytics_records, executemany=True)


    # 테이블 이름 변경 
    @task
    def rename_analytics_table():
        rename_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query)


    # DAG 태스크 연결
    create_raw_table_task = create_raw_table()
    process_data_task = process_data()
    load_raw_table_task = load_raw_table(process_data_task)
    rename_raw_table_task = rename_raw_table()
    create_analytics_table_task = create_analytics_table()
    insert_geocoded_school_data_task = insert_geocoded_school_data()
    rename_analytics_table_task = rename_analytics_table()

    create_raw_table_task >> process_data_task >> load_raw_table_task >> rename_raw_table_task
    rename_raw_table_task >> create_analytics_table_task >> insert_geocoded_school_data_task >> rename_analytics_table_task

seoul_school()
