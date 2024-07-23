from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import logging
import xml.etree.ElementTree as ET
import pendulum
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='cultureSpaceDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    # schedule_interval='10 0 * * *',
    catchup=False
)

# seoul_culture 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.seoul_culture (
    number INT,
    subject_code VARCHAR(100),
    name VARCHAR(100),
    address VARCHAR(200),
    lat FLOAT DEFAULT 0,
    lon FLOAT DEFAULT 0
);
"""

# 데이터 개수 구하는 함수
def getTotalNumber(**context):
    url = 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/culturalSpaceInfo/1/2/'
    response = requests.get(url)
    if response.status_code == 200:
        xml_data = ET.fromstring(str(response.text))
        total_number = xml_data.find('list_total_count').text
    else:
        logging.info("Total Number Error : " + response.status_code)
    logging.info("Got Total Number")
    context['ti'].xcom_push(key='total_number', value=total_number)

# 서울시 문화공간 API에서 데이터 추출 후 CSV로 저장
def extract(**context):
    total_number = int(context['ti'].xcom_pull(key='total_number'))
    logging.info("Extract started")

    data_list = []
    url = context["params"]["url"]
    range_number = 1000
    for i in range(range_number, total_number, range_number):
        url_page = url +  f"""{i- (range_number - 1)}/{i}"""
        response = requests.get(url_page)
        if response.status_code == 200:
            data_list.append(response.text)
        else:
            logging.info("Extract Error : " + response.status_code)

    url_last_page = url + f"""{total_number - (total_number % range_number) +1}/{total_number}"""
    response = requests.get(url_last_page)
    if response.status_code == 200:
        data_list.append(response.text)
    else:
        logging.info("Extract Error : " + response.status_code)

    context['ti'].xcom_push(key='extracted_data', value=data_list)

# 추출한 문화공간 데이터 변환
def transform(**context):
    data_list = context['ti'].xcom_pull(key="extracted_data")
    logging.info("got extract return value")
    logging.info("Transform started")

    trans_list = []
    for data in data_list:
        xml_data = ET.fromstring(data)
        for row in xml_data.findall('row'):
            dict = {}
            dict['number'] = row.find('NUM').text
            dict['subject_code'] = row.find('SUBJCODE').text
            dict['name'] = row.find('FAC_NAME').text
            dict['address'] = row.find('ADDR').text
            dict['lat'] = row.find('X_COORD').text
            dict['lon'] = row.find('Y_COORD').text
            trans_list.append(dict)

    logging.info(trans_list)
    logging.info("Transform ended")
    context['ti'].xcom_push(key='transformed_data', value=trans_list)

# 변환한 데이터 적재하는 함수
def load(**context):
    logging.info("Load start")
    pg_hook = PostgresHook(postgres_conn_id="rs_conn")
    trans_data = context['ti'].xcom_pull(key='transformed_data')

    for data in trans_data:
        number = data['number']
        subject_code = data['subject_code']
        name = data['name']
        address = data['address']
        if data['lat'] == None:
            data['lat'] = 0.0
        lat = data['lat']
        if data['lon'] == None:
            data['lon'] = 0.0
        lon = data['lon']
        insert_query = f"""INSERT INTO raw_data.seoul_culture (number, subject_code, name, address, lat, lon)
                            VALUES ('{number}', '{subject_code}', '{name}', '{address}', '{lat}', '{lon}')"""
        pg_hook.run(insert_query)
    
    logging.info("Load ended")

# seoul_culture 테이블 생성 Task
createCultureTable = PostgresOperator(
    task_id = "create_culture_table",
    postgres_conn_id='rs_conn',
    sql=CREATE_QUERY,
    dag=dag
)

# 데이터 개수 구하는 Task
getCultureTotalNumber = PythonOperator(
    task_id = 'get_culture_total_number',
    python_callable = getTotalNumber,
    dag=dag
)

# 데이터 추출하는 Task
CultureDataExtract = PythonOperator(
    task_id = "culture_extract",
    python_callable=extract,
    params={
        'url': 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/culturalSpaceInfo/'
    },
    dag=dag
)

# 추출한 데이터 변환하는 Task
CultureDataTransform = PythonOperator(
    task_id = "culture_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 데이터 적재하는 Task
CultureDataLoad = PythonOperator(
    task_id="culture_load",
    python_callable=load,
    dag=dag
)

createCultureTable >> getCultureTotalNumber >> CultureDataExtract >> CultureDataTransform >> CultureDataLoad