from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 임시 테이블 생성 task
@task
def create_temp_table(schema, temp_table):
    cur = get_redshift_connection()
    create_query = f"""
    DROP TABLE IF EXISTS {schema}.{temp_table};

    CREATE TABLE {schema}.{temp_table} (
        id INT IDENTITY(1, 1) PRIMARY KEY,
        park_name VARCHAR(100),
        district_name VARCHAR(50),
        address VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT,
        legal_dong_name VARCHAR(50)
    );
    """
    cur.execute(create_query)

# API 요청에 대한 전체 데이터의 개수를 반환하는 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/SearchParkInfoService/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["SearchParkInfoService"]["list_total_count"]
        return total_count
    except requests.RequestException as error:
        logging.error(f"API request failed: {error}")
        raise

# 데이터 수집 task
@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000  # 한 번에 1000개까지 가능
    records = []

    total_count = get_list_total_count()

    # unit_value 단위로 나누어 api 요청
    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/SearchParkInfoService/{start_index}/{end_index}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["SearchParkInfoService"]["row"]
            
            for data in data_list:
                record = (
                    data['P_PARK'],
                    data['P_ZONE'],
                    data['P_ADDR'],
                    data['LATITUDE'],
                    data['LONGITUDE']
                )
                records.append(record)
            
            start_index += unit_value
        except requests.RequestException as error:
            logging.error(f"API request failed: {error}")
            raise

    return records

# 법정동 데이터 추가하는 task
@task
def add_dong_data(records):
    geocoded_records = []
    kakao_api_key = Variable.get('kakao_api_key')

    for record in records:
        latitude = record[3]  
        longitude = record[4]
        url = f'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json?x={longitude}&y={latitude}'
        headers = {"Authorization": kakao_api_key}

        response = requests.get(url, headers=headers)
        dong = None  # 기본값 설정 
        if response.status_code == 200:
            data = response.json()
            try:
                dong = data['documents'][0]['region_3depth_name']
            except (IndexError, KeyError, AttributeError, TypeError):
                pass  # 기본값 유지
            
        geocoded_record = list(record) + [dong]
        geocoded_records.append(tuple(geocoded_record))

    return geocoded_records

# 임시 테이블에 데이터를 적재하는 task
@task
def load_temp_table(schema, temp_table, records):
    cur = get_redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        insert_query = f"""
        INSERT INTO {schema}.{temp_table} (
            park_name, district_name, address, latitude, longitude, legal_dong_name
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise

# 임시 테이블을 원본 테이블로 변경하는 task
@task
def swap_tables(schema, temp_table, main_table):
    cur = get_redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{main_table};")
        cur.execute(f"ALTER TABLE {schema}.{temp_table} RENAME TO {main_table};")
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_park',
    start_date=datetime(2024, 7, 10),  
    schedule_interval='0 1 * * 6',  # 한국 기준 토요일 오전 10시 실행 
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'raw_data'
    temp_table = 'temp_seoul_park'
    main_table = 'seoul_park'

    create_temp_table_task = create_temp_table(schema, temp_table)
    process_task = process_data()
    geocode_task = add_dong_data(process_task)
    load_temp_table_task = load_temp_table(schema, temp_table, geocode_task)
    swap_tables_task = swap_tables(schema, temp_table, main_table)

    create_temp_table_task >> process_task >> geocode_task >> load_temp_table_task >> swap_tables_task
