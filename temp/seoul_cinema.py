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
        screen_name VARCHAR(100),
        status_code VARCHAR(10),
        status_name VARCHAR(40),
        detailed_status_code VARCHAR(10),
        detailed_status_name VARCHAR(40),
        address VARCHAR(255),
        road_address VARCHAR(255),
        last_modified_date DATE, 
        update_type CHAR(1),
        update_date DATE,
        latitude FLOAT,
        longitude FLOAT
    );
    """
    cur.execute(create_query)

# API 요청에 대한 전체 데이터의 개수 반환하는 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/LOCALDATA_031302/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["LOCALDATA_031302"]["list_total_count"]
        return total_count
    
    except requests.RequestException as error:
        print(f"API request failed: {error}")
        raise

# 데이터 수집 task
@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000  # 한 번에 최대 1000건 가능
    records = []

    total_count = get_list_total_count()

    # unit_value 단위로 나누어 api 요청
    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/LOCALDATA_031302/{start_index}/{end_index}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["LOCALDATA_031302"]["row"]
            
            for data in data_list:
                last_modified_date = datetime.strptime(data['LASTMODTS'], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d") if data['LASTMODTS'] else None
                update_date = datetime.strptime(data['UPDATEDT'], '%Y-%m-%d %H:%M:%S.%f').strftime("%Y-%m-%d") if data['UPDATEDT'] else None
                
                record = (
                    data['BPLCNM'],
                    data['TRDSTATEGBN'],
                    data['TRDSTATENM'],
                    data['DTLSTATEGBN'],
                    data['DTLSTATENM'],
                    data['SITEWHLADDR'],
                    data['RDNWHLADDR'],
                    last_modified_date,
                    data['UPDATEGBN'],
                    update_date
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
    api_key = Variable.get('vworld_api_key')
    geocoded_records = []

    for record in records:
        road_address = record[6]  # 도로명 주소 'RDNWHLADDR'
        apiurl = "https://api.vworld.kr/req/address?"
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": road_address,
            "format": "json",
            "type": "road",
            "key": api_key
        }
        response = requests.get(apiurl, params=params)
        if response.status_code == 200:
            try:
                response_json = response.json()
                result = response_json['response']['result']['point']
                latitude = result['y']  # 위도
                longitude = result['x']  # 경도
            except (IndexError, KeyError):
                latitude = None
                longitude = None
            
            geocoded_record = list(record) + [latitude, longitude]
            geocoded_records.append(tuple(geocoded_record))

    return geocoded_records

# redshift 테이블에 적재하는 task
@task
def load(schema, table, records):
    cur = get_redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        insert_query = f"""
        INSERT INTO {schema}.{table} (
            screen_name, status_code, status_name, detailed_status_code, 
            detailed_status_name, address, road_address, last_modified_date, 
            update_type, update_date, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_cinema',
    start_date=datetime(2024, 7, 10),
    schedule_interval='@weekly',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'raw_data'
    table = 'seoul_cinema'

    create = create_table(schema, table)
    records = process_data()
    geocode_task = geocode(records)
    load_task = load(schema, table, geocode_task)

    create >> records >> geocode_task >> load_task
