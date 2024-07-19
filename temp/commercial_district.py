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
        yyyyq VARCHAR(5),
        gu_cd VARCHAR(5),
        gu_nm VARCHAR(20),
        industry_cd VARCHAR(10),
        industry_nm VARCHAR(50),
        store_cnt INT
    );
    """
    cur.execute(create_query)

# ETL 태스크
@task
def etl(schema, table):
    api_key = Variable.get('seoul_api_key')
    start_index = 1
    unit_value = 1000  # index 단위값 설정(한 번에 1000개까지 가능)
    end_index = start_index + unit_value - 1

    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/VwsmSignguStorW/{start_index}/{end_index}"
    response = requests.get(url).json()
    list_total_count = response["VwsmSignguStorW"]["list_total_count"]

    # 첫 번째 요청에서 받은 데이터를 저장
    records = []
    commercial_district_list = response["VwsmSignguStorW"]["row"]
    prev_yyyyq = None
    for list in commercial_district_list:
        current_yyyyq = list['STDR_YYQU_CD']
        record = (
            list['STDR_YYQU_CD'],
            list['SIGNGU_CD'],
            list['SIGNGU_CD_NM'],
            list['SVC_INDUTY_CD'],
            list['SVC_INDUTY_CD_NM'],
            int(list['STOR_CO'])
        )
        # yyyyq 값이 변경되면 멈추기 (yyyyq의 값이 내림차순 정렬이라는 전제.. 수정 필요할 수 있음)
        if prev_yyyyq is not None and current_yyyyq != prev_yyyyq:
            break
        records.append(record)
        prev_yyyyq = current_yyyyq

    # 첫 번째 요청에서 break가 걸리지 않았다면
    # 남은 데이터를 가져오기 위해 반복문 실행
    if len(records) == end_index:
        for next_start_index in range(end_index + 1, list_total_count + 1, unit_value):
            next_end_index = next_start_index + unit_value - 1
            url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/VwsmSignguStorW/{next_start_index}/{next_end_index}"
            response = requests.get(url).json()
            commercial_district_list = response["VwsmSignguStorW"]["row"]

            outer_break = False
            prev_yyyyq = None
            for list in commercial_district_list:
                current_yyyyq = list['STDR_YYQU_CD']
                record = (
                    list['STDR_YYQU_CD'],
                    list['SIGNGU_CD'],
                    list['SIGNGU_CD_NM'],
                    list['SVC_INDUTY_CD'],
                    list['SVC_INDUTY_CD_NM'],
                    int(list['STOR_CO'])
                )
                if prev_yyyyq is not None and current_yyyyq != prev_yyyyq:
                    outer_break = True
                    break   
                records.append(record)
                prev_yyyyq = current_yyyyq
            if outer_break:
                break

    cur = get_Redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")
        insert_query = f"""
        INSERT INTO {schema}.{table} (yyyyq, gu_cd, gu_nm, industry_cd, industry_nm, store_cnt)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_query, record)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='commercial_district',
    start_date=datetime(2024, 7, 10),  
    schedule='0 1 15 * *',  # 매월 15일 오전 1시
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    schema = 'jki5410'
    table = 'commercial_district'

    create_table(schema, table) >> etl(schema, table)