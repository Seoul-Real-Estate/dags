from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import time

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 테이블 생성 task
@task
def create_table(schema, table):
    cur = get_redshift_connection()
    create_query = f"""
    DROP TABLE IF EXISTS {schema}.{table};

    CREATE TABLE {schema}.{table} (
        id INT IDENTITY(1, 1) PRIMARY KEY,
        school_code VARCHAR(7),
        school_name VARCHAR(100),
        school_type VARCHAR(50),
        postal_code VARCHAR(6),
        road_address VARCHAR(150),
        detailed_road_address VARCHAR(150),
        load_date VARCHAR(8),
        latitude FLOAT,
        longitude FLOAT
    );
    """
    cur.execute(create_query)

# API 요청에 대한 전체 데이터의 개수를 반환하는 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisSchoolInfo/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["neisSchoolInfo"]["list_total_count"]
        return total_count
    
    except requests.RequestException as error:
        print(f"API request failed: {error}")
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
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisSchoolInfo/{start_index}/{end_index}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["neisSchoolInfo"]["row"]
            
            for data in data_list:
                record = (
                    data['SD_SCHUL_CODE'],
                    data['SCHUL_NM'],
                    data['SCHUL_KND_SC_NM'],
                    data['ORG_RDNZC'],
                    data['ORG_RDNMA'],
                    data['ORG_RDNDA'],
                    data['LOAD_DTM']
                )
                records.append(record)
            
            start_index += unit_value
        except requests.RequestException as error:
            print(f"API request failed: {error}")
            raise

    return records

# 위/경도 좌표값 추가하는 task
@task
def geocode(records):
    geocoded_records = []
    kakao_api_key = Variable.get('kakao_api_key')

    for record in records:
        road_address = record[4]  # 주소 데이터 'ORG_RDNMA'
        # Kakao API를 사용하여 주소를 좌표로 변환
        url = f'https://dapi.kakao.com/v2/local/search/address.json?query={road_address}&analyze_type=exact'
        headers = {"Authorization": kakao_api_key}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            # 주소 정보에서 좌표 정보 추출
            documents = response.json().get('documents', [])
            if documents:
                latitude = round(float(documents[0].get('y', 0)), 6)  # 소수점 이하 6자리로 반올림
                longitude = round(float(documents[0].get('x', 0)), 6)  # 소수점 이하 6자리로 반올림
            else:
                latitude = None
                longitude = None
            
            geocoded_record = list(record) + [latitude, longitude]
            geocoded_records.append(tuple(geocoded_record))

        except  requests.RequestException as error:
            print(f"Error geocoding: {error}")
            raise

        time.sleep(0.1)  

    return geocoded_records

# redshift 테이블에 적재하는 task
@task
def load(schema, table, records):
    cur = get_redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        insert_query = f"""
        INSERT INTO {schema}.{table} (
            school_code, school_name, school_type, postal_code, 
            road_address, detailed_road_address, load_date, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_school',
    start_date=datetime(2024, 7, 10),  
    schedule_interval='@weekly',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'raw_data'
    table = 'seoul_school'

    create = create_table(schema, table)
    process_task = process_data()
    geocode_task = geocode(process_task)
    load_task = load(schema, table, geocode_task)

    create >> process_task >> geocode_task >> load_task
