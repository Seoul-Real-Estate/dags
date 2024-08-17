from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import logging
import xml.etree.ElementTree as ET
import pendulum

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='seoul_culture',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval='50 9 * * 6',
    catchup=False
)

# redhisft 연결 
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id="redshift_dev")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


# 테이블 생성 
def create_table():
    cursor = get_redshift_connection(autocommit=False)

    try:
        cursor.execute("BEGIN;")
        cursor.execute("""
            DROP TABLE IF EXISTS raw_data.seoul_culture_temp;
            CREATE TABLE raw_data.seoul_culture_temp (
                number INT,
                subject_code VARCHAR(100),
                name VARCHAR(100),
                address VARCHAR(200),
                lat FLOAT DEFAULT 0,
                lon FLOAT DEFAULT 0
            );
        """)
        cursor.execute("COMMIT;")
        logging.info("Table created successfully")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        logging.error(f"Failed to create table: {e}")
        raise
    finally:
        cursor.close()


# 이름 변경
def rename_table():
    cursor = get_redshift_connection(autocommit=False)

    try:
        cursor.execute("BEGIN;")
        cursor.execute("DROP TABLE IF EXISTS raw_data.seoul_culture;")
        cursor.execute("ALTER TABLE raw_data.seoul_culture_temp RENAME TO seoul_culture;")
        cursor.execute("COMMIT;")
        logging.info("Table renamed successfully")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        logging.error(f"Failed to rename table: {e}")
        raise
    finally:
        cursor.close()


# 데이터 개수 구하는 함수
def getTotalNumber(**context):
    url = 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/culturalSpaceInfo/1/2/'
    try:
        response = requests.get(url)
        response.raise_for_status()
        xml_data = ET.fromstring(response.text)
        total_number = xml_data.find('list_total_count').text
        context['ti'].xcom_push(key='total_number', value=total_number)
        logging.info(f"Total Number Retrieved: {total_number}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get total number: {e}")
        raise


# 서울시 문화공간 API에서 데이터 추출 후 CSV로 저장
def extract(**context):
    total_number = int(context['ti'].xcom_pull(key='total_number'))
    logging.info("Extract started")

    data_list = []
    url = context["params"]["url"]
    range_number = 1000

    for i in range(1, total_number + 1, range_number):
        start_num = i
        end_num = min(i + range_number - 1, total_number)
        url_page = f"{url}{start_num}/{end_num}"
        try:
            response = requests.get(url_page)
            response.raise_for_status()
            data_list.append(response.text)
        except requests.exceptions.RequestException as e:
            logging.error(f"Extract Error for range {start_num}-{end_num}: {e}")
            raise

    context['ti'].xcom_push(key='extracted_data', value=data_list)
    logging.info("Extract completed")


# 추출한 문화공간 데이터 변환
def transform(**context):
    data_list = context['ti'].xcom_pull(key="extracted_data")
    logging.info("Transform started")

    trans_list = []

    for data in data_list:
        xml_data = ET.fromstring(data)
        for row in xml_data.findall('row'):
            trans_list.append({
                'number': row.find('NUM').text,
                'subject_code': row.find('SUBJCODE').text,
                'name': row.find('FAC_NAME').text,
                'address': row.find('ADDR').text,
                'lat': row.find('X_COORD').text or 0.0,
                'lon': row.find('Y_COORD').text or 0.0,
            })

    context['ti'].xcom_push(key='transformed_data', value=trans_list)
    logging.info("Transform completed")


# 변환한 데이터 적재하는 함수
def load(**context):
    logging.info("Load started")
    cursor = get_redshift_connection(autocommit=False)

    trans_data = context['ti'].xcom_pull(key='transformed_data')

    try:
        cursor.execute("BEGIN;")

        for data in trans_data:
            number = data['number']
            subject_code = data['subject_code']
            name = data['name']
            address = data['address']
            lat = data.get('lat', 0.0)
            lon = data.get('lon', 0.0)
            insert_query = """
                INSERT INTO raw_data.seoul_culture (number, subject_code, name, address, lat, lon)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (number, subject_code, name, address, lat, lon))
        
        cursor.execute("COMMIT;")
        logging.info("Data loaded successfully")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        logging.error(f"Failed to load data: {e}")
        raise
    finally:
        cursor.close()


# 테이블 생성 및 관리 Task
createTableTask = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

# 데이터 개수 구하는 Task
getCultureTotalNumber = PythonOperator(
    task_id='get_culture_total_number',
    python_callable=getTotalNumber,
    dag=dag
)

# 데이터 추출하는 Task
CultureDataExtract = PythonOperator(
    task_id="culture_extract",
    python_callable=extract,
    params={
        'url': 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/culturalSpaceInfo/'
    },
    dag=dag
)

# 추출한 데이터 변환하는 Task
CultureDataTransform = PythonOperator(
    task_id="culture_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 데이터 적재하는 Task
CultureDataLoad = PythonOperator(
    task_id="culture_load",
    python_callable=load,
    dag=dag
)

# 테이블 이름 변경 Task
renameTableTask = PythonOperator(
    task_id='rename_table',
    python_callable=rename_table,
    dag=dag
)

# DAG trigger
triggerAnalyticsCultureDag = TriggerDagRunOperator(
    task_id="trigger_analytics_culture_dag",
    trigger_dag_id="analytics_seoul_culture",
    wait_for_completion=False,
    reset_dag_run=True,
)

# DAG 구성
createTableTask >> getCultureTotalNumber >> CultureDataExtract >> CultureDataTransform >> CultureDataLoad >> renameTableTask >> triggerAnalyticsCultureDag
