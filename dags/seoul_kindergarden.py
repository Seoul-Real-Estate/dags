from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

RAW_SCHEMA = "raw_data"
ANALYTICS_SCHEMA = "analytics"
TEMP_TABLE = "temp_seoul_kindergarden"
MAIN_TABLE = "seoul_kindergarden"

KAKAO_API_KEY = Variable.get('kakao_api_key')
KAKAO_GEOCODE_URL = 'https://dapi.kakao.com/v2/local/search/address.json'
KAKAO_COORD_TO_REGION_URL = 'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json'

SEOUL_API_KEY = Variable.get("seoul_api_key")
BASE_URL = "http://openapi.seoul.go.kr:8088"
ENDPOINT = "/json/ChildCareInfo"


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

    except Exception as general_error:
        logging.error(f"Exception error: {general_error}")
        cur.execute("ROLLBACK;")
        raise

    finally:
        cur.close()
        conn.close()  


# API 요청에 대한 전체 데이터의 개수를 반환하는 함수
def get_total_data_count():
    url = f"{BASE_URL}/{SEOUL_API_KEY}{ENDPOINT}/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["ChildCareInfo"]["list_total_count"]
        logging.info(f"Total count fetched: {total_count}")
        return total_count
    except requests.RequestException as error:
        logging.error(f"API request failed: {error}")
        raise


# 어린이집 데이터를 API에서 가져오는 함수
def fetch_data(start_index, end_index):
    url = f"{BASE_URL}/{SEOUL_API_KEY}{ENDPOINT}/{start_index}/{end_index}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()["ChildCareInfo"]["row"]
    except requests.RequestException as error:
        logging.error(f"API request failed for range {start_index}-{end_index}: {error}")
        raise


# 데이터를 변환하는 함수
def transform_data(data_list):
    records = []
    for data in data_list:
        latitude = round(float(data["LA"]), 6) if data["LA"] else None
        longitude = round(float(data["LO"]), 6) if data["LO"] else None
        
        record = (
            data["SIGUNNAME"],
            data["CRNAME"],
            data["STCODE"],
            data["CRSTATUSNAME"],
            data["ZIPCODE"],
            data["CRADDR"],
            latitude,
            longitude
        )
        records.append(record)
    logging.info(f"Transformed {len(records)} records.")
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
            if data.get("documents"):
                lat = data["documents"][0]["y"]
                lon = data["documents"][0]["x"]
                return float(lat), float(lon)
            else:
                return None, None
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            logging.error(f"Error processing geocode response address {address}: {e}")
            return None, None
    except requests.RequestException as e:
        logging.error(f"Failed to geocode address {address}: {e}")
        raise


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
        try:
            if data.get("documents"):
                return data['documents'][0]['region_3depth_name'] 
            else:
                return None
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            logging.error(f"Error processing dong name response for coordinates ({x}, {y}): {e}")
            return None
    except requests.RequestException as e:
        logging.error(f"Failed to get dong name for coordinates ({x}, {y}): {e}")
        raise


@dag(
    start_date=datetime(2024, 7, 10),
    schedule_interval="0 10 * * 6",  # 매주 토요일 오전 10시
    catchup=False,
    tags=["weekly", "kindergarden", "raw_data", "analytics", "infra"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def seoul_kindergarden():
    # raw_data 스키마에 테이블 생성 
    @task
    def create_raw_temp_table():
        create_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {RAW_SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            district_name VARCHAR(50),
            kindergarden_name VARCHAR(100),
            kindergarden_code VARCHAR(50),
            status VARCHAR(50),
            postal_code VARCHAR(10),
            road_address VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
        );
        """
        execute_query(create_query)

    # 어린이집 데이터 소스에서 데이터 처리
    @task
    def process_kindergarden_data():
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

    # raw_data 스키마에 어린이집 데이터 적재
    @task
    def load_raw_temp(records):
        insert_query = f"""
        INSERT INTO {RAW_SCHEMA}.{TEMP_TABLE} (
            district_name, kindergarden_name, kindergarden_code, status,
            postal_code, road_address, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        logging.info(f"Loading {len(records)} records into {RAW_SCHEMA}.{TEMP_TABLE}...")
        execute_query(insert_query, parameters=records, autocommit=False, executemany=True)

    # raw_data 스키마의 어린이집 테이블 이름 변경
    @task
    def swap_raw_temp_table():
        swap_query = f"""
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {RAW_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(swap_query, autocommit=False)

    # analytics 스키마에 상태, null값 처리하여 어린이집 테이블 생성 
    @task
    def create_analytics_temp_table():
        create_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{TEMP_TABLE};
        CREATE TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} AS
        SELECT DISTINCT * FROM {RAW_SCHEMA}.{MAIN_TABLE} WHERE status = '정상' AND road_address <> '';
        """
        execute_query(create_query, autocommit=False)

    # analytics 스카마의 어린이집 테이블에 좌표값 없는 행 좌표 데이터 추가
    @task
    def update_missing_coordinates():
        select_query = f"SELECT id, road_address FROM {ANALYTICS_SCHEMA}.{TEMP_TABLE} WHERE latitude IS NULL OR longitude IS NULL"
        rows = execute_query(select_query, fetchall=True)

        for row in rows:
            id, address = row
            latitude, longitude = None, None
            if not address:
                # address가 없을 경우에는 로그를 남기고 업데이트를 생략
                logging.warning(f"Address is missing for id {id}. Skipping update.")
                continue
            latitude, longitude = geocode_address(address)
            update_query = f"""
                UPDATE {ANALYTICS_SCHEMA}.{TEMP_TABLE} SET latitude = %s, longitude = %s
                WHERE id = %s
            """
            execute_query(update_query, (latitude, longitude, id), autocommit=False)

    # analytics 스키마의 어린이집 테이블에 법정동 이름에 대한 컬럼 추가 
    @task
    def add_dong_column():
        alter_query = f"""
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} ADD COLUMN legal_dong_name VARCHAR(30);
        """
        execute_query(alter_query)

    # analytics 스키마의 어린이집 테이블에 법정동 이름 데이터 업데이트 
    @task
    def update_dong_names():
        select_query = f"SELECT id, longitude, latitude FROM {ANALYTICS_SCHEMA}.{TEMP_TABLE}"
        rows = execute_query(select_query, fetchall=True)

        for row in rows:
            id, x, y = row
            dong = get_dong_name(x, y)
            update_query = f"""
                UPDATE {ANALYTICS_SCHEMA}.{TEMP_TABLE} SET legal_dong_name = %s
                WHERE id = %s
            """
            execute_query(update_query, (dong, id), autocommit=False)

    # analytics 스키마의 어린이집 테이블 이름 변경 
    @task
    def swap_analytics_temp_table():
        replace_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(replace_query, autocommit=False)

    create_raw_temp_table_task = create_raw_temp_table()
    process_kindergarden_data_task = process_kindergarden_data()
    load_raw_temp_task = load_raw_temp(process_kindergarden_data_task)
    swap_raw_temp_table_task = swap_raw_temp_table()

    create_analytics_temp_table_task = create_analytics_temp_table()
    update_missing_coordinates_task = update_missing_coordinates()
    add_dong_column_task = add_dong_column()
    update_dong_names_task = update_dong_names()
    swap_analytics_temp_table_task = swap_analytics_temp_table()

    create_raw_temp_table_task >> process_kindergarden_data_task >> load_raw_temp_task >> swap_raw_temp_table_task
    swap_raw_temp_table_task >> create_analytics_temp_table_task >> update_missing_coordinates_task >> add_dong_column_task >> update_dong_names_task >> swap_analytics_temp_table_task

seoul_kindergarden()
