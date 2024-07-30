from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import requests
import logging
import xml.etree.ElementTree as ET
import pendulum

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='subwayDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    # schedule_interval='10 0 * * *',
    catchup=False
)

# seoul_subway 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.seoul_subway (
    station_id VARCHAR(4),
    station_name VARCHAR(100),
    line VARCHAR(50),
    lat FLOAT DEFAULT 0,
    lon FLOAT DEFAULT 0
);
"""

# 서울시 지하철 API에서 데이터 추출
def extract(**context):
    logging.info("Extract started")
    data_list = ''
    url = context["params"]["url"]
    response = requests.get(url)
    if response.status_code == 200:
        data_list = response.text
    else:
        logging.info("Extract Error : " + response.status_code)
    logging.info("Extract done")
    context['ti'].xcom_push(key='extracted_data', value=data_list)

# 추출한 지하철 데이터 변환
def transform(**context):
    extract_data = context['ti'].xcom_pull(task_ids='subway_extract')
    logging.info("got extract return value")
    logging.info("Transform started")
    trans_list = []
    data = ET.fromstring(extract_data)
    for row in data.findall('row'):
        dict = {}
        dict['station_id'] = row.find('BLDN_ID').text
        dict['station_nm'] = row.find('BLDN_NM').text
        dict['line'] = row.find('ROUTE').text
        dict['lat'] = row.find('LAT').text
        dict['lon'] = row.find('LOT').text
        trans_list.append(dict)
    logging.info(trans_list)
    logging.info("Transform ended")
    return trans_list

# 지하철 데이터 적재
def load(**context):
    logging.info("Load start")
    pg_hook = PostgresHook(postgres_conn_id="rs_conn")
    trans_data = context['ti'].xcom_pull(task_ids='subwayStation_transform')
    for data in trans_data:
        id = data['station_id']
        name = data['station_nm']
        line = data['line']
        lat = data['lat']
        lon = data['lon']
        insert_query = f"""INSERT INTO raw_data.seoul_subway (station_id, station_name, line, lat, lon)
                            VALUES ('{id}', '{name}', '{line}', '{lat}', '{lon}')"""
        pg_hook.run(insert_query)
    logging.info("Load ended")

# seoul_subway 테이블 생성 Task
createSubwayTable = PostgresOperator(
    task_id = "create_subway_table",
    postgres_conn_id='rs_conn',
    sql=CREATE_QUERY,
    dag=dag
)

# 지하철 데이터 추출하는 Task
subwayDataExtract = PythonOperator(
    task_id = "subway_extract",
    python_callable=extract,
    params={
        'url': 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/subwayStationMaster/1/800/'
    },
    dag=dag
)

# 추출한 데이터 변환하는 Task
subwayDataTransform = PythonOperator(
    task_id = "subway_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 데이터 적재하는 Task
subwayDataLoad = PythonOperator(
    task_id="subway_load",
    python_callable=load,
    dag=dag
)

createSubwayTable >> subwayDataExtract >> subwayDataTransform >> subwayDataLoad
