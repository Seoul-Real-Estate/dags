from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import requests
import logging
from xml.dom.minidom import parseString
import xml.etree.ElementTree as ET
import pendulum

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='mountainParkDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    # schedule_interval='10 0 * * *',
    catchup=False
)

# seoul_mountain_park 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS seoul_mountain_park (
    key VARCHAR(30),
    name VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(20),
    gu VARCHAR(20),
    dong VARCHAR(20)
);
"""

# API에서 데이터 추출하는 함수
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
    return data_list

# 추출한 데이터 변환하는 함수
def transform(**context):
    extract_data = context['ti'].xcom_pull(task_ids='mountainPark_extract')
    logging.info("got extract return value")
    logging.info("Transform started")
    trans_list = []
    data = ET.fromstring(extract_data)
    for row in data.findall('row'):
        if row.find('CATE1_NAME').text == "산과공원":
            dict = {}
            dict['key'] = row.find('MAIN_KEY').text
            dict['name'] = row.find('NAME_KOR').text
            dict['address'] = row.find('ADD_KOR').text
            dict['city'] = row.find('H_KOR_CITY').text
            dict['gu'] = row.find('H_KOR_GU').text
            dict['dong'] = row.find('H_KOR_DONG').text
            trans_list.append(dict)
    logging.info(trans_list)
    logging.info("Transform ended")
    return trans_list

# 변환한 데이터 적재하는 함수
def load(**context):
    logging.info("Load start")
    pg_hook = PostgresHook(postgres_conn_id="myRedshift")
    trans_data = context['ti'].xcom_pull(task_ids='mountainPark_transform')
    for data in trans_data:
        key = data['key']
        name = data['name']
        address = data['address']
        city = data['city']
        gu = data['gu']
        dong = data['dong']
        insert_query = f"""INSERT INTO seoul_mountain_park (key, name, address, city, gu, dong)
                            VALUES ('{key}', '{name}', '{address}', '{city}', '{gu}', '{dong}')"""
        
        pg_hook.run(insert_query)
    
    logging.info("Load ended")

# seoul_mountain_park 테이블 생성 Task
createMountainParkTable = PostgresOperator(
    task_id = "create_mountainPark_table",
    postgres_conn_id='myRedshift',
    sql=CREATE_QUERY,
    dag=dag
)

# 산과 공원에 대한 데이터 추출하는 Task
mountainParkDataExtract = PythonOperator(
    task_id = "mountainPark_extract",
    python_callable=extract,
    params={
        'url': 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/SebcParkTourKor/1/200/'
    },
    dag=dag
)

# 추출한 데이터 변환하는 Task
mountainParkDataTransform = PythonOperator(
    task_id = "mountainPark_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 데이터 적재하는 Task
mountainParkDataLoad = PythonOperator(
    task_id="mountainPark_load",
    python_callable=load,
    dag=dag
)

createMountainParkTable >> mountainParkDataExtract >> mountainParkDataTransform >> mountainParkDataLoad