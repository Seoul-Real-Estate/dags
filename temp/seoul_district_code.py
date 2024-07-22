from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def create_table(schema, table):
    cur = get_Redshift_connection()
    create_query = f"""
    DROP TABLE IF EXISTS {schema}.{table};
    CREATE TABLE {schema}.{table} (
        id INT IDENTITY(1, 1),
        district_code INT,
        legal_dong_code INT,
        administrative_dong_code INT,
        city_name VARCHAR(50),
        district_name VARCHAR(50),
        legal_dong_name VARCHAR(50),
        administrative_dong_name VARCHAR(50)
    );
    """
    cur.execute(create_query)

def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bigCmpBjdongMgmInfo/1/1"
    response = requests.get(url).json()
    return response["bigCmpBjdongMgmInfo"]["list_total_count"]

@task
def process_data(unit_value):
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    all_records = []

    total_count = get_list_total_count()

    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bigCmpBjdongMgmInfo/{start_index}/{end_index}"
        response = requests.get(url).json()
        
        data_list = response["bigCmpBjdongMgmInfo"]["row"]
        for data in data_list:
            record = (
                int(data['SGG_CD']),
                int(data['STDG_CD']),
                int(data['DONG_CD']),
                data['CTPV_NM'],
                data['SGG_NM'],
                data['STDG_LI_NM'],
                data['DONG_NM']
            )
            all_records.append(record)
        
        start_index += unit_value

    return all_records

@task
def load(schema, table, records):
    cur = get_Redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")
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
        print(error)
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
    schema = 'jki5410'
    table = 'seoul_district_code'

    create_table_task = create_table(schema, table)
    process_data_task = process_data(unit_value=1000)  # 한 번에 1000건까지 가능
    load_task = load(schema, table, process_data_task)

    create_table_task >> process_data_task >> load_task
