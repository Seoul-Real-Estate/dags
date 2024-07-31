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
        longitude FLOAT
    );
    """
    cur.execute(create_query)

# API 요청 응답 대한 전체 데이터의 개수를 반환하는 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisAcademyInfo/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["neisAcademyInfo"]["list_total_count"]
        return total_count
    except requests.RequestException as error:
        print(f"API request failed: {error}")
        raise

# 데이터 수집 task
@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value=1000
    records = []

    total_count = get_list_total_count()

    # unit_value 단위로 나누어 api 요청
    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisAcademyInfo/{start_index}/{end_index}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["neisAcademyInfo"]["row"]
            
            for data in data_list:
                record = (
                    data['ADMST_ZONE_NM'],
                    data['ACA_ASNUM'],
                    data['ACA_NM'],
                    int(data['TOFOR_SMTOT']),
                    data['REALM_SC_NM'],
                    data['LE_CRSE_NM'],
                    data['FA_RDNZC'],
                    data['FA_RDNMA'],
                    data['FA_RDNDA']
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
        road_address = record[7]  # 주소값 'FA_RDNMA'
        if not road_address:
            latitude = None
            longitude = None
            geocoded_records.append(list(record) + [latitude, longitude])
            continue
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

        except Exception as error:
            print(f"Error geocoding address {road_address}: {error}")

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
            district_name, academy_code, academy_name, 
            total_capacity, category_name, course_name,
            postal_code, road_address, detailed_road_address,
            latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_academy',
    start_date=datetime(2024, 7, 10),
    schedule_interval='@weekly',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'raw_data'
    table = 'seoul_academy'

    create_table_task = create_table(schema, table)
    process_data_task = process_data()  
    geocode_task = geocode(process_data_task)
    load_task = load(schema, table, geocode_task)

    create_table_task >> process_data_task >> geocode_task >> load_task
