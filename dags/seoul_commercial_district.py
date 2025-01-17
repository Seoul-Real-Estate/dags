from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2

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
        yyyyq VARCHAR(5),
        district_code VARCHAR(5),
        district_name VARCHAR(20),
        industry_code VARCHAR(10),
        industry_name VARCHAR(50),
        store_count INT
    );
    """
    cur.execute(create_query)

# 전체 데이터의 수 반환 함수
def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/VwsmSignguStorW/1/1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        total_count = data["VwsmSignguStorW"]["list_total_count"]
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
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/VwsmSignguStorW/{start_index}/{end_index}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list = response.json()["VwsmSignguStorW"]["row"]
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
        except requests.RequestException as error:
            print(f"API request failed: {error}")
            raise

    return records

# redshift 테이블에 적재하는 task
@task
def load(schema, table, records):
    cur = get_redshift_connection(False)
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
        print(f"Database error: {error}")
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_commercial_district',
    start_date=datetime(2024, 7, 10),  
    schedule='0 1 15 * *',  # 한국 기준 매월 15일 오전 10시
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
