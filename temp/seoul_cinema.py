from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
from geopy.geocoders import Nominatim
import time

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def create_table(schema, table):
    cur = get_Redshift_connection()
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

def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/LOCALDATA_031302/1/1"
    response = requests.get(url).json()
    total_count = response["LOCALDATA_031302"]["list_total_count"]
    return total_count

@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000  # 한 번에 최대 1000건 가능
    records = []

    total_count = get_list_total_count()

    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/LOCALDATA_031302/{start_index}/{end_index}"
        response = requests.get(url).json()
        
        data_list = response["LOCALDATA_031302"]["row"]
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

    return records

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

@task
def load(schema, table, records):
    cur = get_Redshift_connection(False)
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
    schedule_interval='0 0 * * 6',  # 매주 금요일 정각에 실행
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
