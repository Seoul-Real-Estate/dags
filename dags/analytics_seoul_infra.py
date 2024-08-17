from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import psycopg2

ANALYTICS_SCHEMA = "analytics"
TEMP_TABLE = "temp_seoul_infra"
MAIN_TABLE = "seoul_infra"

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn


# 쿼리문 실행시키는 함수
def execute_query(query, parameters=None, autocommit=True, fetchall=False, executemany=False):
    conn = get_redshift_connection(autocommit)
    cur = conn.cursor()
    try:
        if not autocommit:
            cur.execute("BEGIN;")
        
        if parameters:
            if executemany:
                cur.executemany(query, parameters)
            else:
                cur.execute(query, parameters)
        else:
            cur.execute(query)
        
        if not autocommit:
            cur.execute("COMMIT;")
        
        if fetchall:
            return cur.fetchall()
    except psycopg2.DatabaseError as db_error:
        logging.error(f"Database error: {db_error}")
        cur.execute("ROLLBACK;")
        raise
    except Exception as error:
        print(f"execute query failed {error}, {type(error)}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


@dag(
    start_date=datetime(2024, 7, 10),
    catchup=False,
    schedule_interval="0 18 * * 6",  
    tags=["analytics", "infra", "weekly"],
    default_args={
        "owner": "kain",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def analytics_seoul_infra():

    # 테이블 생성
    @task
    def create_temp_table():
        create_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{TEMP_TABLE};

        CREATE TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} (
            id INT IDENTITY(1, 1) PRIMARY KEY,
            category VARCHAR(50),
            name VARCHAR(100),
            address VARCHAR(255),
            detailed_address VARCHAR(255) DEFAULT NULL,
            latitude FLOAT,
            longitude FLOAT,
            district_name VARCHAR(30),
            legal_dong_name VARCHAR(30),
            culture_subject VARCHAR(50) DEFAULT NULL 
        );
        """
        execute_query(create_query)


    # 데이터 삽입
    @task
    def insert_data():
        # (category, {컬럼명 매핑}, 테이블명)
        # key: 통합 테이블의 컬럼 이름, value: 각각의 테이블의 컬럼 이름
        tables = [
            ("문화시설", {
                "name": "name",
                "address": "address",
                "detailed_address": "NULL",
                "latitude": "latitude",
                "longitude": "longitude",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "culture_subject"
            }, "analytics.seoul_culture"),
            ("병원", {
                "name": "name",
                "address": "address",
                "detailed_address": "NULL",
                "latitude": "lat",
                "longitude": "lon",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "NULL"
            }, "analytics.seoul_hospital"),
            ("어린이집", {
                "name": "kindergarden_name",
                "address": "road_address",
                "detailed_address": "NULL",
                "latitude": "latitude",
                "longitude": "longitude",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "NULL"
            }, "analytics.seoul_kindergarden"),
            ("학원", {
                "name": "academy_name",
                "address": "road_address",
                "detailed_address": "detailed_road_address",
                "latitude": "latitude",
                "longitude": "longitude",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "NULL"
            }, "raw_data.seoul_academy"),
            ("영화관", {
                "name": "screen_name",
                "address": "road_address",
                "detailed_address": "NULL",
                "latitude": "latitude",
                "longitude": "longitude",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "NULL"
            }, "analytics.seoul_cinema"),
            ("공원", {
                "name": "park_name",
                "address": "address",
                "detailed_address": "NULL",
                "latitude": "latitude",
                "longitude": "longitude",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "NULL"
            }, "raw_data.seoul_park"),
            ("학교", {
                "name": "school_name",
                "address": "road_address",
                "detailed_address": "detailed_road_address",
                "latitude": "latitude",
                "longitude": "longitude",
                "district_name": "district_name",
                "legal_dong_name": "legal_dong_name",
                "culture_subject": "NULL"
            }, "analytics.seoul_school")
        ]
        
        for category, column_mapping, table_location in tables:
            values = ", ".join([col for col in column_mapping.values()])
            
            insert_query = f"""
            INSERT INTO {ANALYTICS_SCHEMA}.{TEMP_TABLE} (category, name, address, detailed_address, latitude, longitude, district_name, legal_dong_name, culture_subject)
            SELECT '{category}' AS category, {values}
            FROM {table_location}
            WHERE (district_name <> '') AND
                (legal_dong_name IS NOT NULL) AND
                (legal_dong_name <> '');
            """
            execute_query(insert_query, autocommit=False)


    # 테이블 이름 변경 
    @task
    def rename_table():
        rename_query = f"""
        DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{MAIN_TABLE};
        ALTER TABLE {ANALYTICS_SCHEMA}.{TEMP_TABLE} RENAME TO {MAIN_TABLE};
        """
        execute_query(rename_query, autocommit=False)

    create_temp_table_task = create_temp_table()
    insert_data_task = insert_data()
    rename_table_task = rename_table()

    create_temp_table_task >> insert_data_task >> rename_table_task

analytics_seoul_infra()
