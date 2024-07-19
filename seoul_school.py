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
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        school_code VARCHAR(7),
        school_name VARCHAR(100),
        school_type VARCHAR(50),
        postal_code VARCHAR(5),
        road_address VARCHAR(150),
        detailed_road_address VARCHAR(150),
        load_date VARCHAR(8)
    );
    """
    cur.execute(create_query)

# ETL 태스크
@task
def extract():
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000  # index 단위값 설정(한 번에 1000개까지 가능)
    end_index = start_index + unit_value - 1

    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisSchoolInfo/{start_index}/{end_index}"
    response = requests.get(url).json()
    return {'response': response, 'unit_value': unit_value, 'end_index': end_index}

@task
def transform(extracted_data):
    response = extracted_data['response']
    unit_value = extracted_data['unit_value']
    end_index = extracted_data['end_index']
    
    list_total_count = response["neisSchoolInfo"]["list_total_count"]

    # 첫 번째 요청에서 받은 데이터를 저장
    records = []
    school_list = response["neisSchoolInfo"]["row"]
    for school in school_list:
        record = (
            school['SD_SCHUL_CODE'],
            school['SCHUL_NM'],
            school['SCHUL_KND_SC_NM'],
            school['ORG_RDNZC'],
            school['ORG_RDNMA'],
            school['ORG_RDNDA'],
            school['LOAD_DTM']
        )
        records.append(record)

    # 남은 데이터를 가져오기 위해 반복문 실행
    for next_start_index in range(end_index + 1, list_total_count + 1, unit_value):
        next_end_index = next_start_index + unit_value - 1
        api_key = Variable.get('seoul_api_key')
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/neisSchoolInfo/{next_start_index}/{next_end_index}"
        response = requests.get(url).json()
        school_list = response["neisSchoolInfo"]["row"]

        for school in school_list:
            record = (
                school['SD_SCHUL_CODE'],
                school['SCHUL_NM'],
                school['SCHUL_KND_SC_NM'],
                school['ORG_RDNZC'],
                school['ORG_RDNMA'],
                school['ORG_RDNDA'],
                school['LOAD_DTM']
            )
            records.append(record)
    return records

@task
def load(schema, table, records):
    cur = get_Redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")
        insert_query = f"""
        INSERT INTO {schema}.{table} (school_code, school_name, school_type, postal_code, road_address, detailed_road_address, load_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='seoul_school',
    start_date=datetime(2024, 7, 10),  
    schedule_interval='0 0 * * 6',  # 매주 금요일 정각에 실행
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'jki5410'
    table = 'seoul_school'

    create_table_task = create_table(schema, table)
    extract_task = extract()
    transform_task = transform(extract_task)  # Extract 함수의 반환값을 개별 인자로 전달
    load_task = load(schema, table, transform_task)

    create_table_task >> extract_task >> transform_task >> load_task
