from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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


kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='busStationDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval='@yearly',
    catchup=False
)

# seoul_bus_station 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.seoul_bus_station (
    station_id VARCHAR(20),
    gu VARCHAR(20),
    station_name VARCHAR(200),
    total_route INT
);
"""

# 서울시 버스 정보 csv 파일로 다운받는 함수
def extract(**context):
    logging.info("Extract started")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")  # Headless 모드 설정
    chrome_options.add_argument("--download.default_directory=/downloads")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": "/opt/airflow/downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    driver.get("https://data.seoul.go.kr/dataList/OA-22187/F/1/datasetView.do")

    wait = WebDriverWait(driver, 20)
    button_1 = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="fileTr_1"]/td[6]/a')))
    button_1.click()
    print("Button clicked successfully!")

    time.sleep(10)

    download_dir = '/opt/airflow/downloads'
    files = glob.glob(os.path.join(download_dir, "서울시 시내버스 정류소 현황_*"))
    if files:
        latest_file = max(files, key=os.path.getctime)
        new_name = os.path.join(download_dir, "seoul_bus_station.xlsx")
        os.rename(latest_file, new_name)
        logging.info(f"File renamed to: {new_name}")
    else:
        logging.info("No files found for renaming")


    file_name = 'seoul_bus_station.xlsx'
    
    file_path = f'/opt/airflow/downloads/{file_name}'
    
    file = Path(file_path)
    if file.is_file():
        logging.info("Download Succeed")
    else:
        logging.info("Download Failed")

    driver.quit() 
    logging.info("Extract done")
    return file_name

# 다운받은 CSV 파일 변환하는 함수
def transform(**context):
    logging.info("transform started")
    global xlsx_file_name, xlsx_file_path
    xlsx_file_name = context['ti'].xcom_pull(task_ids="bus_extract")
    try:
        xlsx_file_path = f'downloads/{xlsx_file_name}'
        
        xlsx = pd.read_excel(xlsx_file_path)

        new_file_path = 'downloads/seoul_bus_station.csv'
        xlsx.to_csv(new_file_path)

        df = pd.read_csv(new_file_path)
        
        df.columns = ['0', '구분', '자치구', 'ID', '정류소 명', '노선수', '6', '7', '8', '9', '10']
        df = df.drop(['0', '구분','6', '7', '8', '9', '10'], axis=1)
        df.to_csv(new_file_path, index=False, header=False)

        logging.info("transform finished")   
        logging.info("new file path : " + new_file_path)
        return new_file_path   
      
    except Exception as e:
        logging.info(f"An error occurred: {e}")

# 변환한 CSV 파일 S3에 적재하는 함수
def upload_to_S3(file_path, **kwargs):
    bucket_name = 'team-ariel-2-data'
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename=file_path,
        key='data/seoul_bus_station.csv', 
        bucket_name= bucket_name, 
        replace=True
    )

# S3에서 Redshift로 COPY해서 적재하는 함수
def load_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id='rs_conn')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # Redshift용 COPY 명령문
    copy_query = f"""
    COPY raw_data.seoul_bus_station
    FROM 's3://team-ariel-2-data/data/seoul_bus_station.csv'
    IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
    CSV
    """
    
    cursor.execute(copy_query)
    conn.commit()
    cursor.close()

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

createBusStationTable >> busDataExtract >> busDataTransform >> upload_data_to_S3 >> load_data_to_redshift
