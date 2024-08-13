from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 임시 테이블 생성
@task
def create_temp_table():
    cur = get_redshift_connection()
    try:
        create_query = """
        DROP TABLE IF EXISTS analytics.temp_seoul_infra;

        CREATE TABLE analytics.temp_seoul_infra (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            category VARCHAR(50),
            name VARCHAR(100),
            address VARCHAR(255),
            detailed_address VARCHAR(255) DEFAULT NULL,
            latitude FLOAT,
            longitude FLOAT,
            district_name VARCHAR(30),
            legal_dong_name VARCHAR(30),
            subject_code VARCHAR(50) DEFAULT NULL 
        );
        """
        cur.execute(create_query)
    except Exception as e:
        logging.error(f"Error creating table: {e}")

# 데이터 삽입
@task
def insert_data():
    cur = get_redshift_connection(False)
    try:
        tables = [
            # (category, (컬럼명 매핑), (테이블명))
            # key: 통합 테이블의 컬럼 이름, value: 원본 테이블의 컬럼 이름
            # detailed_address와 subject_code는 모든 테이블에 있는 컬럼이 아니므로 NULL값 처리
            ("문화시설", {
                'name': 'name',
                'address': 'address',
                'detailed_address': 'NULL',
                'latitude': 'lat',
                'longitude': 'lon',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'subject_code'
            }, "analytics.seoul_culture"),
            ("병원", {
                'name': 'name',
                'address': 'address',
                'detailed_address': 'NULL',
                'latitude': 'lat',
                'longitude': 'lon',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'NULL'
            }, "analytics.seoul_hospital"),
            ("어린이집", {
                'name': 'kindergarden_name',
                'address': 'road_address',
                'detailed_address': 'NULL',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'NULL'
            }, "analytics.seoul_kindergarden"),
            ("학원", {
                'name': 'academy_name',
                'address': 'road_address',
                'detailed_address': 'detailed_road_address',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'NULL'
            }, "raw_data.seoul_academy"),
            ("영화관", {
                'name': 'screen_name',
                'address': 'road_address',
                'detailed_address': 'NULL',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'NULL'
            }, "raw_data.seoul_cinema"),
            ("공원", {
                'name': 'park_name',
                'address': 'address',
                'detailed_address': 'NULL',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'NULL'
            }, "raw_data.seoul_park"),
            ("학교", {
                'name': 'school_name',
                'address': 'road_address',
                'detailed_address': 'detailed_road_address',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'district_name': 'district_name',
                'legal_dong_name': 'legal_dong_name',
                'subject_code': 'NULL'
            }, "raw_data.seoul_school")
        ]
        
        cur.execute("BEGIN;")
        for category, column_mapping, table_location in tables:
            values = ', '.join([col for col in column_mapping.values()])
            
            insert_query = f"""
            INSERT INTO analytics.temp_seoul_infra (category, name, address, detailed_address, latitude, longitude, district_name, legal_dong_name, subject_code)
            SELECT 
                '{category}' AS category,
                {values}
            FROM {table_location};
            """
            cur.execute(insert_query)
        
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        cur.execute("ROLLBACK;")
        raise

# 테이블 교체
@task
def swap_tables():
    cur = get_redshift_connection(False)
    try:
        cur.execute("BEGIN;")
        cur.execute("DROP TABLE IF EXISTS analytics.seoul_infra;")
        cur.execute("ALTER TABLE analytics.temp_seoul_infra RENAME TO seoul_infra;")
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(f"Error swapping tables: {e}")
        cur.execute("ROLLBACK;")

# DAG 정의
with DAG(
    dag_id='analytics_seoul_infra',
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 6 * * 6',  # 한국 기준 토요일 오후 3시
    catchup=False
) as dag:
    create_temp_table_task = create_temp_table()
    insert_data_task = insert_data()
    swap_tables_task = swap_tables()

    create_temp_table_task >> insert_data_task >> swap_tables_task
