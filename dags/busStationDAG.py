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
FILE_NAME = "seoul_bus_station.csv"
DOWNLOAD_FILE_NAME = "서울시 버스정류소 위치정보.csv"


kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="busStationDAG",
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval="0 4 * * 3#1",
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
    logging.info("Extract 함수 시작")

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
    button_1 = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='btnCsv']")))
    button_1.click()
    logging.info("다운로드 버튼 클릭")

    time.sleep(10)

    files = glob.glob(os.path.join(airflow_path, DOWNLOAD_FILE_NAME))
    if files:
        latest_file = max(files, key=os.path.getctime)
        new_name = os.path.join(airflow_path, FILE_NAME)
        os.rename(latest_file, new_name)
        logging.info(f"{DOWNLOAD_FILE_NAME}에서 {new_name}로 파일 이름이 수정됨")
    else:
        logging.info(f"이름을 변경할 {DOWNLOAD_FILE_NAME} 파일이 없음")
    
    file_path = f"{airflow_path}/{FILE_NAME}"
    
    file = Path(file_path)
    if file.is_file():
        logging.info(f"{file_path} 경로에 다운로드 성공")
    else:
        logging.info(f"다운로드 실패 : {file_path}에 파일이 존재하지 않음")

    driver.quit() 
    logging.info("Extract 함수 종료")
    return file_path


# 다운받은 CSV 파일 변환하는 함수
def transform(**context):
    logging.info("transform 함수 시작")
    try:
        file_path = context["ti"].xcom_pull(task_ids="bus_extract")
        logging.info(f"파일 경로 extract로 부터 가져오기 성공 : {file_path}")
        df = pd.read_csv(file_path, encoding="cp949")
        
        df.columns = ["노드ID", "정류소번호", "정류소명", "X", "Y", "정류소타입"]
        df = df.drop(["노드ID", "정류소타입"], axis=1)
        df.to_csv(file_path, index=False, header=False)

        logging.info("데이터 변환한 csv 파일 경로 : " + file_path)
        logging.info("transform 함수 마무리")   
        return file_path   
    except Exception as e:
        logging.info(f"에러 발생: {e}")


# 변환한 CSV 파일 S3에 적재하는 함수
def upload_to_S3(file_path, **kwargs):
    bucket_name = "team-ariel-2-data"
    hook = S3Hook(aws_conn_id="S3_conn")
    hook.load_file(
        filename=file_path,
        key=f"data/{FILE_NAME}", 
        bucket_name= bucket_name, 
        replace=True
    )


# S3에서 Redshift로 COPY해서 적재하는 함수
def load_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id="rs_conn")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    aws_iam_role = Variable.get("aws_iam_role")

    # Redshift용 COPY 명령문
    copy_query = f"""
    COPY raw_data.seoul_bus_station
    FROM "s3://team-ariel-2-data/data/{FILE_NAME}"
    IAM_ROLE "{aws_iam_role}"
    CSV
    """
    
    cursor.execute(copy_query)
    conn.commit()
    cursor.close()

# seoul_bus_station 테이블 삭제하는 Task
delete_bus_station_table = PostgresOperator(
    task_id = "delete_bus_station_table",
    postgres_conn_id="rs_conn",
    sql=DELETE_QUERY,
    dag=dag
)

# seoul_bus_station 테이블 생성하는 Task
create_bus_station_table = PostgresOperator(
    task_id = "create_bus_station_table",
    postgres_conn_id="rs_conn",
    sql=CREATE_QUERY,
    dag=dag
)

# 서울시 버스 정보 csv 파일로 다운받는 Task
bus_data_extract = PythonOperator(
    task_id = "bus_extract",
    python_callable=extract,
    dag=dag
)

# 다운받은 CSV 파일 변환하는 Task
bus_data_transform = PythonOperator(
    task_id = "bus_transform",
    python_callable=transform,
    dag=dag
)

# 변환한 CSV 파일 S3에 적재하는 Task
upload_data_to_S3 = PythonOperator(
    task_id = "upload_to_S3",
    python_callable=upload_to_S3,
    op_kwargs={"file_path": "{{ task_instance.xcom_pull(task_ids='bus_transform') }}"}
)

# S3에서 Redshift로 COPY해서 적재하는 Task
load_data_to_redshift = PythonOperator(
    task_id = "load_to_redshift",
    python_callable=load_to_redshift
)

delete_bus_station_table >> create_bus_station_table >> bus_data_extract >> bus_data_transform >> upload_data_to_S3 >> load_data_to_redshift
