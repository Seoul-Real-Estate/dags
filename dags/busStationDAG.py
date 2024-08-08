from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import time
from pathlib import Path
import logging
import pandas as pd

from datetime import datetime
import logging
import pendulum
import os
import glob

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL = "https://data.seoul.go.kr/dataList/OA-15067/S/1/datasetView.do"
FILE_NAME = 'seoul_bus_station.csv'


kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='busStationDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval='0 4 * * 3#1',
    catchup=False
)
DELETE_QUERY = "DROP TABLE IF EXISTS raw_data.seoul_bus_station"

# seoul_bus_station 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.seoul_bus_station (
    station_id VARCHAR(20),
    station_name VARCHAR(200),
    longitude FLOAT,
    latitude FLOAT
);
"""

# 서울시 버스 정보 csv 파일로 다운받는 함수
def extract(**context):
    logging.info("Extract started")

    airflow_path = Variable.get("airflow_download_path")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")  # Headless 모드 설정
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": airflow_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    driver.get(URL)

    wait = WebDriverWait(driver, 20)
    button_1 = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="btnCsv"]')))
    button_1.click()
    logging.info("Button 1 clicked successfully!")

    time.sleep(10)

    files = glob.glob(os.path.join(airflow_path, "서울시 버스정류소 위치정보.csv"))
    if files:
        latest_file = max(files, key=os.path.getctime)
        new_name = os.path.join(airflow_path, FILE_NAME)
        os.rename(latest_file, new_name)
        logging.info(f"File renamed to: {new_name}")
    else:
        logging.info("No files found for renaming")
    
    file_path = f'{airflow_path}/{FILE_NAME}'
    
    file = Path(file_path)
    if file.is_file():
        logging.info("Download Succeed")
    else:
        logging.info("Download Failed")

    driver.quit() 
    logging.info("Extract done")
    return file_path

# 다운받은 CSV 파일 변환하는 함수
def transform(**context):
    logging.info("transform started")
    try:
        file_path = context['ti'].xcom_pull(task_ids="bus_extract")
        logging.info(file_path)
        df = pd.read_csv(file_path, encoding='cp949')
        
        df.columns = ['노드ID', '정류소번호', '정류소명', 'X', 'Y', '정류소타입']
        df = df.drop(['노드ID', '정류소타입'], axis=1)
        df.to_csv(file_path, index=False, header=False)

        logging.info("transform finished")   
        logging.info("new file path : " + file_path)
        return file_path   
      
    except Exception as e:
        logging.info(f"An error occurred: {e}")

# 변환한 CSV 파일 S3에 적재하는 함수
def upload_to_S3(file_path, **kwargs):
    bucket_name = 'team-ariel-2-data'
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename=file_path,
        key=f'data/{FILE_NAME}', 
        bucket_name= bucket_name, 
        replace=True
    )

# S3에서 Redshift로 COPY해서 적재하는 함수
def load_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id='rs_conn')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    aws_iam_role = Variable.get('aws_iam_role')

    # Redshift용 COPY 명령문
    copy_query = f"""
    COPY raw_data.seoul_bus_station
    FROM 's3://team-ariel-2-data/data/{FILE_NAME}'
    IAM_ROLE '{aws_iam_role}'
    CSV
    """
    
    cursor.execute(copy_query)
    conn.commit()
    cursor.close()

# seoul_bus_station 테이블 생성하는 Task
deleteBusStationTable = PostgresOperator(
    task_id = "delete_bus_station_table",
    postgres_conn_id='rs_conn',
    sql=DELETE_QUERY,
    dag=dag
)

# seoul_bus_station 테이블 생성하는 Task
createBusStationTable = PostgresOperator(
    task_id = "create_bus_station_table",
    postgres_conn_id='rs_conn',
    sql=CREATE_QUERY,
    dag=dag
)

# 서울시 버스 정보 csv 파일로 다운받는 Task
busDataExtract = PythonOperator(
    task_id = "bus_extract",
    python_callable=extract,
    dag=dag
)

# 다운받은 CSV 파일 변환하는 Task
busDataTransform = PythonOperator(
    task_id = "bus_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 CSV 파일 S3에 적재하는 Task
upload_data_to_S3 = PythonOperator(
    task_id = "upload_to_S3",
    python_callable=upload_to_S3,
    op_kwargs={'file_path': '{{ task_instance.xcom_pull(task_ids="bus_transform") }}'}
)

# S3에서 Redshift로 COPY해서 적재하는 Task
load_data_to_redshift = PythonOperator(
    task_id = "load_to_redshift",
    python_callable=load_to_redshift
)

deleteBusStationTable >> createBusStationTable >> busDataExtract >> busDataTransform >> upload_data_to_S3 >> load_data_to_redshift
