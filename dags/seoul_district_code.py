from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2

# Redshift 연결 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 테이블 생성 task
@task
def create_table(schema, table):
    cur = get_Redshift_connection()
    create_query = f"""
    DROP TABLE IF EXISTS {schema}.{table};
    CREATE TABLE {schema}.{table} (
        id INT IDENTITY(1, 1) PRIMARY KEY,
        district_code VARCHAR(10),
        legal_dong_code VARCHAR(10),
        administrative_dong_code VARCHAR(10),
        city_name VARCHAR(50),
        district_name VARCHAR(50),
        legal_dong_name VARCHAR(50),
        administrative_dong_name VARCHAR(50)
    );
    """
    cur.execute(create_query)

# 전체 데이터의 수 반환 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bigCmpBjdongMgmInfo/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["bigCmpBjdongMgmInfo"]["list_total_count"]
        return total_count
    except requests.RequestException as error:
        print(f"API request failed: {error}")
        raise

# 데이터 수집 task
@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1 
    unit_value = 1000  # 한 번에 1000건까지 가능
    records = []

    total_count = get_list_total_count()

    # unit_value 단위로 나누어 api 요청
    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bigCmpBjdongMgmInfo/{start_index}/{end_index}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["bigCmpBjdongMgmInfo"]["row"]
            for data in data_list:
                record = (
                    data['SGG_CD'],
                    data['STDG_CD'],
                    data['DONG_CD'],
                    data['CTPV_NM'],
                    data['SGG_NM'],
                    data['STDG_LI_NM'],
                    data['DONG_NM']
                )
                records.append(record)
            start_index += unit_value
        except requests.RequestException as error:
            print(f"API request failed: {error}")
            raise

    return records

# redshift 테이블에 적재하는 task
@task
def load(schema, table, records):
    cur = get_Redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        insert_query = f"""
        INSERT INTO {schema}.{table} (
            district_code, legal_dong_code, administrative_dong_code, 
            city_name, district_name, legal_dong_name, administrative_dong_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_district_code',
    start_date=datetime(2024, 7, 10),
    schedule_interval='0 1 1 * *',  # 매월 1일 오전 1시에 실행
    catchup=False,
    default_args={
        'retries': 1,  
        'retry_delay': timedelta(minutes=5),  
    }
) as dag:
    schema = 'raw_data'
    table = 'seoul_district_code'

    create_table_task = create_table(schema, table)
    process_data_task = process_data()  
    load_task = load(schema, table, process_data_task)

    create_table_task >> process_data_task >> load_task