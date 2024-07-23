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
        district_name VARCHAR(50),
        academy_code VARCHAR(50),
        academy_name VARCHAR(100),
        total_capacity INT,
        category_name VARCHAR(100),
        course_name VARCHAR(100),
        postal_code VARCHAR(6),
        road_address VARCHAR(255),
        detailed_road_address VARCHAR(255)
    );
    """
    cur.execute(create_query)

def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisAcademyInfo/1/1"
    response = requests.get(url).json()
    return response["neisAcademyInfo"]["list_total_count"]

@task
def process_data(unit_value):
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    all_records = []

    total_count = get_list_total_count()

    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisAcademyInfo/{start_index}/{end_index}"
        response = requests.get(url).json()
        
        data_list = response["neisAcademyInfo"]["row"]
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
            district_name, academy_code, academy_name, 
            total_capacity, category_name, course_name,
            postal_code, road_address, detailed_road_address
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
    dag_id='seoul_academy',
    start_date=datetime(2024, 7, 10),
    schedule_interval='0 1 1 * *',  # 매월 1일 오전 1시에 실행
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'jki5410'
    table = 'seoul_academy'

    create_table_task = create_table(schema, table)
    process_data_task = process_data(unit_value=1000)  # 한 번에 1000건까지 가능
    load_task = load(schema, table, process_data_task)

    create_table_task >> process_data_task >> load_task
