from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2

# Redshift 연결 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 테이블 생성 태스크
@task
def create_table(schema, table):
    cur = get_Redshift_connection()
    create_query = f"""
    drop table if exists {schema}.{table};
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
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
        tm_x DECIMAL(20, 9),
        tm_y DECIMAL(20, 9)
    );
    """
    cur.execute(create_query)

def get_list_total_count():
    api_key = Variable.get('seoul_api_key')
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/LOCALDATA_031302/1/1"
    response = requests.get(url).json()
    return response["LOCALDATA_031302"]["list_total_count"]

@task
def process_data(unit_value):
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    all_records = []

    total_count = get_list_total_count()

    while start_index <= total_count:
        end_index = min(start_index + unit_value - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/LOCALDATA_031302/{start_index}/{end_index}"
        response = requests.get(url).json()
        
        cinema_list = response["LOCALDATA_031302"]["row"]
        for cinema in cinema_list:
            last_modified_date = datetime.strptime(cinema['LASTMODTS'], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d") if cinema['LASTMODTS'] else None
            update_date = datetime.strptime(cinema['UPDATEDT'], '%Y-%m-%d %H:%M:%S.%f').strftime("%Y-%m-%d") if cinema['UPDATEDT'] else None
            tm_x = float(cinema['X']) if cinema['X'] else None
            tm_y = float(cinema['Y']) if cinema['Y'] else None

            record = (
                cinema['BPLCNM'],
                cinema['TRDSTATEGBN'],
                cinema['TRDSTATENM'],
                cinema['DTLSTATEGBN'],
                cinema['DTLSTATENM'],
                cinema['SITEWHLADDR'],
                cinema['RDNWHLADDR'],
                last_modified_date,
                cinema['UPDATEGBN'],
                update_date,
                tm_x,
                tm_y
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
            screen_name, status_code, status_name, detailed_status_code, 
            detailed_status_name, address, road_address, last_modified_date, 
            update_type, update_date, tm_x, tm_y
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
    schedule_interval='0 1 * * *',  # 매일 오전 1시에 실행
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'jki5410'
    table = 'seoul_cinema'
    unit_value = 1000  # index 단위값 설정(한 번에 1000개까지 가능)

    create = create_table(schema, table)
    all_records = process_data(unit_value)
    load_task = load(schema, table, all_records)

    create >> all_records >> load_task