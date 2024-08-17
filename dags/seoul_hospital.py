from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
from datetime import datetime
import requests
import logging
import xml.etree.ElementTree as ET
import pendulum
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='seoul_hospital_v2',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule_interval='0 0 28 * *',
    catchup=False
)

# seoul_hospital 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.seoul_hospital (
    id VARCHAR(10),
    address VARCHAR(200),
    type_name VARCHAR(30),
    name VARCHAR(100),
    zip_code_1 INT,
    zip_code_2 INT,
    lat FLOAT DEFAULT 0,
    lon FLOAT DEFAULT 0
);
"""

# 데이터 개수 구하는 함수
def getTotalNumber():
    url = 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/TbHospitalInfo/1/2'
    response = requests.get(url)
    if response.status_code == 200:
        xml_data = ET.fromstring(str(response.text))
        total_number = xml_data.find('list_total_count').text
    else:
        logging.info("Total Number Error : " + response.status_code)
    logging.info("Got Total Number")
    return total_number

# 서울시 병의원 API에서 데이터 추출 후 CSV로 저장
def extract(**context):
    total_number = int(context["ti"].xcom_pull(task_ids="get_hospital_total_number"))
    logging.info("Extract started")

    data_list = []
    url = context["params"]["url"]
    range_number = 1000
    for i in range(range_number, total_number, range_number):
        url_page = f"{url}{i - (range_number - 1)}/{i}"
        response = requests.get(url_page)
        if response.status_code == 200:
            data_list.append(response.text)
        else:
            logging.error("Extract Error: " + str(response.status_code))
            raise Exception(f"Failed to extract data for range {i - (range_number - 1)}-{i}")

    url_last_page = f"{url}{total_number - (total_number % range_number) + 1}/{total_number}"
    response = requests.get(url_last_page)
    if response.status_code == 200:
        data_list.append(response.text)
    else:
        logging.error("Extract Error: " + str(response.status_code))
        raise Exception("Failed to extract data for last page")

    df_list = []
    for data in data_list:
        xml_data = ET.fromstring(data)
        for row in xml_data.findall("row"):
            record = {
                "id": row.find("HPID").text,
                "address": row.find("DUTYADDR").text,
                "type_name": row.find("DUTYDIVNAM").text,
                "name": row.find("DUTYNAME").text,
                "zip_code_1": row.find("POSTCDN1").text,
                "zip_code_2": row.find("POSTCDN2").text,
                "lat": row.find("WGS84LAT").text,
                "lon": row.find("WGS84LON").text
            }
            df_list.append(record)
    
    df = pd.DataFrame(df_list)
    airflow_path = Variable.get("airflow_download_path")
    file_path = f"{airflow_path}/seoul_hospital.csv"
    df.to_csv(file_path, index=False)
    logging.info("Extract done")
    return file_path

# CSV 파일 S3로 업로드하는 함수
def upload_to_S3(file_path, **kwargs):
    bucket_name = 'team-ariel-2-data'
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename=file_path,
        key='data/seoul_hospital.csv', 
        bucket_name= bucket_name, 
        replace=True
    )

# S3에서 Redshift로 COPY해서 적재하는 함수
def load_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id='redshift_dev')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # Redshift용 COPY 명령문
    copy_query = f"""
    COPY raw_data.seoul_hospital
    FROM 's3://team-ariel-2-data/data/seoul_hospital.csv'
    IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
    CSV
    IGNOREHEADER 1;
    """

    cursor.execute(copy_query)
    conn.commit()
    cursor.close()



# hospital 테이블 생성 Task
createHospitalTable = PostgresOperator(
    task_id = "create_hospital_table",
    postgres_conn_id='redshift_dev',
    sql=CREATE_QUERY,
    dag=dag
)

# 데이터 개수 구하는 Task
getHospitalTotalNumber = PythonOperator(
    task_id = 'get_hospital_total_number',
    python_callable = getTotalNumber,
    dag=dag
)

# 데이터 추출 후 CSV로 저장하는 Task
hospitalDataExtract = PythonOperator(
    task_id = "hospital_extract",
    python_callable=extract,
    params={
        'url': 'http://openapi.seoul.go.kr:8088/574263537a736a653131326e49736b65/xml/TbHospitalInfo/'
    },
    dag=dag
)

# CSV 파일 S3에 저장하는 Task
upload_data_to_S3 = PythonOperator(
    task_id = "upload_to_S3",
    python_callable=upload_to_S3,
    op_kwargs={'file_path': '{{ task_instance.xcom_pull(task_ids="hospital_extract") }}'}
)

# S3에서 Redshift로 COPY해서 적재하는 Task
load_data_to_redshift = PythonOperator(
    task_id = "load_to_redshift",
    python_callable=load_to_redshift
)

# analytics_seoul_hospital 트리거
trigger_analytics_hospital_dag = TriggerDagRunOperator(
    task_id="trigger_analytics_hospital_dag",
    trigger_dag_id="analytics_seoul_hospital",
    wait_for_completion=False,
    reset_dag_run=True,
    dag=dag
)


createHospitalTable >> getHospitalTotalNumber >> hospitalDataExtract >> upload_data_to_S3 >> load_data_to_redshift >> trigger_analytics_hospital_dag