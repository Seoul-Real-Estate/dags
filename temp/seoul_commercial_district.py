from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2

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
        yyyyq VARCHAR(5),
        district_code VARCHAR(5),
        district_name VARCHAR(20),
        industry_code VARCHAR(10),
        industry_name VARCHAR(50),
        store_count INT
    );
    """
    cur.execute(create_query)

def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/VwsmSignguStorW/1/1"
    response = requests.get(url).json()
    total_count = response["VwsmSignguStorW"]["list_total_count"]
    return total_count

@task
def process_data():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000  # 한 번에 1000개까지 가능
    records = []
    total_count = get_list_total_count()

    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/VwsmSignguStorW/{start_index}/{end_index}"
        response = requests.get(url).json()
        
        data_list = response["VwsmSignguStorW"]["row"]
        for data in data_list:
            record = (
                data['STDR_YYQU_CD'],
                data['SIGNGU_CD'],
                data['SIGNGU_CD_NM'],
                data['SVC_INDUTY_CD'],
                data['SVC_INDUTY_CD_NM'],
                int(data['STOR_CO'])
            )
            records.append(record)
        
        start_index += unit_value

    return records

@task
def load(schema, table, records):
    cur = get_Redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        insert_query = f"""
        INSERT INTO {schema}.{table} (
            yyyyq, district_code, district_name, industry_code, industry_name, store_count
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_commercial_district',
    start_date=datetime(2024, 7, 10),  
    schedule='0 1 15 * *',  # 매월 15일 오전 1시
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'raw_data'
    table = 'seoul_commercial_district'

    create_table_task = create_table(schema, table)
    process_data_task = process_data()  
    load_task = load(schema, table, process_data_task)

    create_table_task >> process_data_task >> load_task
