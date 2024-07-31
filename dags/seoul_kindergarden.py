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
        kindergarden_name VARCHAR(100),
        kindergarden_code VARCHAR(50),
        status VARCHAR(50),
        postal_code VARCHAR(10),
        detailed_address VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT
    );
    """
    
    cur.execute(create_query)

# API 요청에 대한 전체 데이터의 개수를 반환하는 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/ChildCareInfo/1/1"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["ChildCareInfo"]["list_total_count"]
        return total_count
    
    except requests.RequestException as error:
        print(f"API request failed: {error}")
        raise

# 데이터 수집 task
@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000
    records = []

    total_count = get_list_total_count()

    # unit_value 단위로 나누어 api 요청
    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/ChildCareInfo/{start_index}/{end_index}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["ChildCareInfo"]["row"]
            
            for data in data_list:
                latitude = round(float(data['LA']), 6) if data['LA'] else None
                longitude = round(float(data['LO']), 6) if data['LO'] else None
                
                record = (
                    data['SIGUNNAME'],
                    data['CRNAME'],
                    data['STCODE'],
                    data['CRSTATUSNAME'],
                    data['ZIPCODE'],
                    data['CRADDR'],
                    latitude,
                    longitude
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
        if not record[6] or not record[7]:  # 위/경도 값이 없는 경우
            road_address = record[5]  # 주소 데이터 'CRADDR'
            
            if not road_address:
                geocoded_records.append(tuple(list(record[:6]) + [None, None]))
                continue
            
            url = f'https://dapi.kakao.com/v2/local/search/address.json?query={road_address}&analyze_type=exact'
            headers = {"Authorization": kakao_api_key}
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                # 주소 정보에서 좌표 정보 추출
                documents = data.get('documents', [])
                if documents:
                    latitude = round(float(documents[0].get('y', 0)), 6)
                    longitude = round(float(documents[0].get('x', 0)), 6)
                else:
                    latitude = None
                    longitude = None
                
                geocoded_record = list(record[:6]) + [latitude, longitude]
                geocoded_records.append(tuple(geocoded_record))
            except Exception as error:
                print(f"Error geocoding address {road_address}: {error}")
                raise
            
            time.sleep(0.1) 
        else:
            # 이미 위/경도 값이 있는 경우 그대로 추가
            geocoded_records.append(record)

    return geocoded_records

# Redshift 테이블에 적재하는 task
@task
def load(schema, table, records):
    cur = get_redshift_connection(autocommit=False)
    
    try:
        cur.execute("BEGIN;")
        insert_query = f"""
        INSERT INTO {schema}.{table} (
            district_name, kindergarden_name, kindergarden_code, status,
            postal_code, detailed_address, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for record in records:
            cur.execute(insert_query, record)
        
        cur.execute("COMMIT;")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_kindergarden',
    start_date=datetime(2024, 7, 10),
    schedule_interval='@weekly',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'raw_data'
    table = 'seoul_kindergarden'

    create = create_table(schema, table)
    records = process_data()
    geocode_task = geocode(records)
    load_task = load(schema, table, geocode_task)

    create >> records >> geocode_task >> load_task