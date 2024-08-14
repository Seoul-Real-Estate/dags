from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
import time

RAW_SCHEMA = "raw_data"
ANALYTICS_SCHEMA = "analytics"
TEMP_TABLE = "temp_seoul_cinema"
MAIN_TABLE = "seoul_cinema"

SEOUL_API_KEY = Variable.get("seoul_api_key")
API_BASE_URL = "http://openapi.seoul.go.kr:8088"
API_RESPONSE_TYPE = "json/"
API_ENDPOINT = "LOCALDATA_031302"

VWORLD_API_KEY = Variable.get("vworld_api_key")
GEOCODE_URL = "https://api.vworld.kr/req/address?"


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
        print(f"execute query failed {error}, {type(error)}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


# API 요청에 대한 전체 데이터의 개수를 반환하는 함수
def get_total_data_count():
    url = f"{API_BASE_URL}/{SEOUL_API_KEY}/{API_RESPONSE_TYPE}{API_ENDPOINT}/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data[API_ENDPOINT]["list_total_count"]
        logging.info(f"Total count fetched: {total_count}")
        return total_count
    except Exception as error:
        logging.error(f"API request failed: {error}, {type(error)}")
        raise


# 영화상영관 데이터를 API에서 가져오는 함수
def fetch_seoul_data(start_index, end_index):
    url = f"{API_BASE_URL}/{SEOUL_API_KEY}/{API_RESPONSE_TYPE}{API_ENDPOINT}/{start_index}/{end_index}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()[API_ENDPOINT]["row"]
    except Exception as error:
        logging.error(f"API request failed: {error}, {type(error)}")
        raise


# 데이터 변환
def transform_data(data_list):
    records = []
    for data in data_list:
        last_modified_date = datetime.strptime(data["LASTMODTS"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d") if data["LASTMODTS"] else None
        update_date = datetime.strptime(data["UPDATEDT"], "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d") if data["UPDATEDT"] else None
        
        record = (
            data["BPLCNM"],
            data["TRDSTATEGBN"],
            data["TRDSTATENM"],
            data["DTLSTATEGBN"],
            data["DTLSTATENM"],
            data["SITEWHLADDR"],
            data["RDNWHLADDR"],
            last_modified_date,
            data["UPDATEGBN"],
            update_date
        )
        records.append(record)
    logging.info(f"Transformed {len(records)} records.")
    return records


# 주소값을 통해 좌표와 행정구역 데이터를 받아오는 함수
def geocode(road_address):
    params = {
        "service": "address",
        "request": "getcoord",
        "crs": "epsg:4326",
        "address": road_address,
        "format": "json",
        "type": "road",
        "key": VWORLD_API_KEY
    }

    try:
        response = requests.get(GEOCODE_URL, params=params)
        response.raise_for_status()

        try:
            response_json = response.json()
            result = response_json["response"]
            latitude = result["result"]["point"]["y"]
            longitude = result["result"]["point"]["x"]
            district_name = result["refined"]["structure"]["level2"]
            legal_dong_name = result["refined"]["structure"]["level3"]
        except (IndexError, KeyError, TypeError, AttributeError) as error:
            logging.error(f"Failed to parse geocoding response for address: {road_address} with error: {error}")
            latitude = None
            longitude = None
            district_name = None
            legal_dong_name = None
    except Exception as error:
        logging.error(f"API request failed for address: {road_address}, {error}")
        raise

    return latitude, longitude, district_name, legal_dong_name


@dag(
    start_date=datetime(2024, 7, 10),
    schedule_interval="0 10 * * 6",  # 매주 토요일 오전 10시
    catchup=False,
    tags=["weekly", "cinema", "raw_data", "analytics", "infra"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def seoul_cinema():

    # raw_data 스키마에 테이블 생성 
    @task
    def create_raw_table():
        create_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {RAW_SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            screen_name VARCHAR(100),
            status_code VARCHAR(10),
            status VARCHAR(40),
            detailed_status_code VARCHAR(10),
            detailed_status VARCHAR(40),
            address VARCHAR(255),
            road_address VARCHAR(255),
            last_modified_date DATE, 
            update_type CHAR(1),
            update_date DATE,
            latitude FLOAT,
            longitude FLOAT,
            district_name VARCHAR(30),
            legal_dong_name VARCHAR(30)
        );
        """
        execute_query(create_query)

    # 원본 데이터 수집 및 변형 
    @task
    def process_raw_data():
        total_count = get_total_data_count()
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

    # 좌표, 행정구역 데이터 추가 
    @task
    def add_coordinate_and_dong(records):
        geocoded_records = []
        batch_size = 100  
        pause_duration = 300  
        request_delay = 2  

        for i, record in enumerate(records):
            address = record[6]

            latitude, longitude, district_name, legal_dong_name = geocode(address)

            geocoded_record = list(record) + [latitude, longitude, district_name, legal_dong_name]
            geocoded_records.append(tuple(geocoded_record))

            time.sleep(request_delay)

            if (i + 1) % batch_size == 0:
                time.sleep(pause_duration)

        logging.info(f"Geocoded {len(geocoded_records)} records in total.")
        return geocoded_records

    # 원본 데이터 적재
    @task
    def load_raw_temp(records):
        insert_query = f"""
        INSERT INTO {RAW_SCHEMA}.{TEMP_TABLE} (
            screen_name, status_code, status, detailed_status_code, 
            detailed_status, address, road_address, last_modified_date, 
            update_type, update_date, latitude, longitude, district_name, legal_dong_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, records, autocommit=False, executemany=True)

    # 테이블 이름 변경 
    @task
    def swap_raw_temp_table():
        replace_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {RAW_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(replace_query, autocommit=False)

    # analytics 스키마에 필터링된 테이블 생성 
    @task
    def create_analytics_table():
    # 1. "영업중"이며 주소가 있는 데이터만 필터링
    # 2. 같은 영화관의 여러 상영관 데이터를 하나로 통합 (좌표별로 최신 데이터(업데이트 날짜 기준)만 남기고 중복 제거)
        create_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{MAIN_TABLE};
        CREATE TABLE {ANALYTICS_SCHEMA}.{MAIN_TABLE} AS
        WITH FilteredData AS (
            SELECT *
            FROM raw_data.seoul_cinema
            WHERE detailed_status = '영업중' AND road_address <> ''
        ),
        RankedData AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY latitude, longitude ORDER BY update_date DESC) AS rn
            FROM FilteredData
        )
        SELECT *
        FROM RankedData
        WHERE rn = 1;
        """
        execute_query(create_query, autocommit=False)

    create_raw_table_task = create_raw_table()
    process_raw_data_task = process_raw_data()
    add_coordinate_and_dong_task = add_coordinate_and_dong(process_raw_data_task)
    load_raw_temp_task = load_raw_temp(add_coordinate_and_dong_task)
    swap_raw_temp_table_task = swap_raw_temp_table()
    create_analytics_table_task = create_analytics_table()

    create_raw_table_task >> process_raw_data_task >> add_coordinate_and_dong_task >> load_raw_temp_task >> swap_raw_temp_table_task >> create_analytics_table_task

seoul_cinema()
