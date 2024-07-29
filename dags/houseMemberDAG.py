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
    dag_id='houseMemberDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval='@yearly',
    catchup=False
)

# seoul_house_member 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.seoul_house_member (
    gu VARCHAR(20),
    total INT,
    one_person INT,
    two_person INT,
    three_person INT,
    four_person INT,
    five_person INT,
    six_person INT,
    seven_person INT,
    average FLOAT
);
"""

# 서울시 가구수 정보 csv 파일로 다운받는 함수
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

    driver.get("https://data.seoul.go.kr/dataList/10597/S/2/datasetView.do")

    wait = WebDriverWait(driver, 20)
    iframe = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="IframeRequest"]')))

    driver.switch_to.frame(iframe)

    button_1 = driver.find_element(By.XPATH, '//*[@id="ico_download"]/a')
    button_1.click()
    logging.info("Button clicked successfully!")

    time.sleep(5)

    button_2 = driver.find_element(By.XPATH, '//*[@id="csvradio"]')
    button_2.click()
    time.sleep(2)
    logging.info("Button clicked successfully!_2")

    button_3 = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pop_downgrid2"]/div[2]/div[3]/span[1]/a')))
    button_3.click()
    logging.info("Button clicked successfully!_3")

    time.sleep(10)

    download_dir = '/opt/airflow/downloads'
    files = glob.glob(os.path.join(download_dir, "가구원수별+가구-+읍면동(연도+끝자리+0,5),+시군구(그+외+연도)_202407*"))
    if files:
        latest_file = max(files, key=os.path.getctime)
        new_name = os.path.join(download_dir, "seoul_house_member_202406.csv")
        os.rename(latest_file, new_name)
        logging.info(f"File renamed to: {new_name}")
    else:
        logging.info("No files found for renaming")


    file_name = 'seoul_house_member_202406.csv'
    
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
    global csv_file_name, csv_file_path
    csv_file_name = context['ti'].xcom_pull(task_ids="house_extract")
    try:
        csv_file_path = f'downloads/{csv_file_name}'
        df = pd.read_csv(csv_file_path)
        
        logging.info("DataFrame loaded successfully")
        df.columns = ['지워', '구', '합계', '1인가구', '2인가구', '3인가구', '4인가구', '5인가구', '6인가구', '7인이상가구', '평균 가구원수']
        df = df.drop([0, 1])
        df = df.drop('지워', axis=1)
        df.to_csv(csv_file_path, index=False, header=False)
        logging.info("transform finished")
        return csv_file_path
        
    except Exception as e:
        logging.info(f"An error occurred: {e}")

# 변환한 CSV 파일 S3에 적재하는 함수
def upload_to_S3(file_path, **kwargs):
    bucket_name = 'team-ariel-2-data'
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename=file_path,
        key='data/seoul_house_member_202406.csv', 
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
    COPY raw_data.seoul_house_member
    FROM 's3://team-ariel-2-data/data/seoul_house_member_202406.csv'
    IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
    CSV
    """
    
    cursor.execute(copy_query)
    conn.commit()
    cursor.close()

# seoul_house_member 테이블 생성하는 Task
createHouseTable = PostgresOperator(
    task_id = "create_house_table",
    postgres_conn_id='rs_conn',
    sql=CREATE_QUERY,
    dag=dag
)

# 서울시 가구수 정보 csv 파일로 다운받는 Task
houseDataExtract = PythonOperator(
    task_id = "house_extract",
    python_callable=extract,
    dag=dag
)

# 다운받은 CSV 파일 변환하는 Task
houseDataTransform = PythonOperator(
    task_id = "house_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 CSV 파일 S3에 적재하는 Task
upload_data_to_S3 = PythonOperator(
    task_id = "upload_to_S3",
    python_callable=upload_to_S3,
    op_kwargs={'file_path': '{{ task_instance.xcom_pull(task_ids="house_transform") }}'}
)

# S3에서 Redshift로 COPY해서 적재하는 Task
load_data_to_redshift = PythonOperator(
    task_id = "load_to_redshift",
    python_callable=load_to_redshift
)

createHouseTable >> houseDataExtract >> houseDataTransform >> upload_data_to_S3 >> load_data_to_redshift
