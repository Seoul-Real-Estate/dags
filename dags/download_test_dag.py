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
    dag_id='download_test_DAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval='@yearly',
    catchup=False
)

def extract(**context):
    logging.info("Extract started")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")  # Headless 모드 설정
    chrome_options.add_argument("--download.default_directory=/downloads")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": "../downloads",
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

    download_dir = '../downloads'
    files = glob.glob(os.path.join(download_dir, "서울시 시내버스 정류소 현황_*"))
    if files:
        latest_file = max(files, key=os.path.getctime)
        new_name = os.path.join(download_dir, "seoul_bus_station.xlsx")
        os.rename(latest_file, new_name)
        logging.info(f"File renamed to: {new_name}")
    else:
        logging.info("No files found for renaming")


    file_name = 'seoul_bus_station.xlsx'
    
    file_path = f'../downloads/{file_name}'
    
    file = Path(file_path)
    if file.is_file():
        logging.info("Download Succeed")
    else:
        logging.info("Download Failed")

# 서울시 버스 정보 csv 파일로 다운받는 Task
busDataExtract = PythonOperator(
    task_id = "data_extract",
    python_callable=extract,
    dag=dag
)

busDataExtract