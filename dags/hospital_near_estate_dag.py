from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

from datetime import datetime
import requests
import logging
import xml.etree.ElementTree as ET
import pendulum
import json
import pandas as pd
import math
import numpy as np

FILE_NAME = "hospital_near_estate.csv"
BUCKET_NAME = Variable.get('bucket_name')
IAM_ROLE = Variable.get('aws_iam_role')

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='hospital_near_estate_dag',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    catchup=False
)

#  테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.hospital_near_estate (
    estate_id VARCHAR(500),
    x FLOAT,
    y FLOAT,
    type VARCHAR(500),
    hospital_name_1 VARCHAR(500),
    hospital_type_1 VARCHAR(500),
    longitude_1 FLOAT, 
    latitude_1 FLOAT,
    hospital_name_2 VARCHAR(500),
    hospital_type_2 VARCHAR(500),
    longitude_2 FLOAT, 
    latitude_2 FLOAT,
    hospital_name_3 VARCHAR(500),
    hospital_type_3 VARCHAR(500),
    longitude_3 FLOAT, 
    latitude_3 FLOAT,
    hospital_name_4 VARCHAR(500),
    hospital_type_4 VARCHAR(500),
    longitude_4 FLOAT, 
    latitude_4 FLOAT,
    hospital_name_5 VARCHAR(500),
    hospital_type_5 VARCHAR(500),
    longitude_5 FLOAT, 
    latitude_5 FLOAT,
    hospital_name_6 VARCHAR(500),
    hospital_type_6 VARCHAR(500),
    longitude_6 FLOAT, 
    latitude_6 FLOAT
);
"""

def getDataCount(**context):
    redshift_hook = RedshiftSQLHook(redshift_conn_id='rs_conn')
    
    sql = """
    SELECT COUNT(*) FROM raw_data.hospital_near_estate;
    """
    
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()        

    context["ti"].xcom_push(key="data_cnt", value=rows[0][0])


def decideNextTask(**context):
    cnt = context['ti'].xcom_pull(key="data_cnt")

    if int(cnt) == 0:
        return 'extract_all_estate'
    else:
        return 'extract_unique_estate'
    
    
def extractAllEstate(**context):
    logging.info("estractAllEstate")

    redshift_hook = RedshiftSQLHook(redshift_conn_id='rs_conn')
    
    sql = """
        SELECT id, longitude, latitude FROM raw_data.real_estate;
        """
        
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
        
    df = pd.DataFrame(rows, columns=['id', 'longitude', 'latitude'])
    arr = df.values.tolist()
    length = len(arr)

    context['ti'].xcom_push(key="estate_data_length", value=length)
    context["ti"].xcom_push(key="estate_data", value=arr)


def extractUniqueEstate(**context):
    logging.info("extractUniqueEstate")

    redshift_hook = RedshiftSQLHook(redshift_conn_id='rs_conn')
    
    new_estate_sql = """
    SELECT A.id, A.longitude, A.latitude FROM raw_data.real_estate AS A
    LEFT JOIN raw_data.hospital_near_estate AS B
    ON A.id = B.estate_id 
    WHERE B.estate_id IS NULL;
    """
        
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(new_estate_sql)
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=['id', 'longitude', 'latitude'])
    arr = df.values.tolist()
    length = len(arr)

    context['ti'].xcom_push(key="estate_data_length", value=length)
    context["ti"].xcom_push(key="estate_data", value=arr)


def transformEstateData(**context):
    logging.info("getEstateXY")
    data = context["ti"].xcom_pull(key="estate_data")
    length = context["ti"].xcom_pull(key="estate_data_length")

    data_number = length // 6
    end = 0
    for i in range(5):
        start = i * data_number
        end = (i + 1) * data_number
        arr = data[start: end]
        context["ti"].xcom_push(key=f"estate_transform_data_{i+1}", value=arr)
    
    arr = data[end: length]
    context["ti"].xcom_push(key=f"estate_transform_data_6", value=arr)


def hospital_extract(**context):
   redshift_hook = RedshiftSQLHook(redshift_conn_id='rs_conn')
    
   sql = """
    SELECT name, type_name, lon, lat
    FROM analytics.seoul_hospital
    """
    
   conn = redshift_hook.get_conn()
   cursor = conn.cursor()
   cursor.execute(sql)
   rows = cursor.fetchall()

   context["ti"].xcom_push(key="hospital_extracted_data", value=rows)


def hospital_transform_1(**context):
    hospital_extracted_list = context["ti"].xcom_pull(key="hospital_extracted_data")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_1")

    hospital_transformed_data = []
    hospital_df = pd.DataFrame(hospital_extracted_list, columns=['hospital_name', 'hospital_type', 'longitude', 'latitude'])
    address_df = pd.DataFrame(address_list, columns=['id', 'longitude', 'latitude'])

    for index_1, row_1 in address_df.iterrows():
        a = float(row_1['longitude'])
        b = float(row_1['latitude'])
        id = str(row_1['id'])
        hospital_df["longitude_sub"] = hospital_df["longitude"] - a
        hospital_df["latitude_sub"] = hospital_df["latitude"] - b
        hospital_df["longitude_sub"] = hospital_df["longitude_sub"].abs()
        hospital_df["latitude_sub"] = hospital_df["latitude_sub"].abs()

        hospital_df = hospital_df.sort_values(by=['latitude', 'longitude'], ascending=[True, True])
        
        closest_hospitals = pd.DataFrame()
        for hospital_type in hospital_df["hospital_type"].unique():
            type_df = hospital_df[hospital_df["hospital_type"] == hospital_type]
            type_df = type_df.sort_values(by=['longitude_sub', 'latitude_sub'], ascending=[True, True])
            logging.info(type_df)
            type_df = type_df[:2]
            closest = type_df.drop(labels=["longitude_sub", "latitude_sub"], axis=1)
            closest_hospitals = pd.concat([closest_hospitals, closest])

        hospital_list = closest_hospitals.values.tolist()
        # logging.info(hospital_list)

        hospital_data = []
        hospital_data.append(str(id))
        hospital_data.append(a)
        hospital_data.append(b)
        hospital_data.append("병원")
        for i in range(len(hospital_list)):
            hospital_data.append(hospital_list[i][0])
            hospital_data.append(hospital_list[i][1])
            hospital_data.append(hospital_list[i][2])
            hospital_data.append(hospital_list[i][3])
        hospital_transformed_data.append(hospital_data)

    context["ti"].xcom_push(key="hospital_transformed_data_1", value=hospital_transformed_data)

def hospital_transform_2(**context):
    hospital_extracted_list = context["ti"].xcom_pull(key="hospital_extracted_data")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_2")

    hospital_transformed_data = []
    hospital_df = pd.DataFrame(hospital_extracted_list, columns=['hospital_name', 'hospital_type', 'longitude', 'latitude'])
    address_df = pd.DataFrame(address_list, columns=['id', 'longitude', 'latitude'])

    for index_1, row_1 in address_df.iterrows():
        a = float(row_1['longitude'])
        b = float(row_1['latitude'])
        id = str(row_1['id'])
        hospital_df["longitude_sub"] = hospital_df["longitude"] - a
        hospital_df["latitude_sub"] = hospital_df["latitude"] - b
        hospital_df["longitude_sub"] = hospital_df["longitude_sub"].abs()
        hospital_df["latitude_sub"] = hospital_df["latitude_sub"].abs()

        hospital_df = hospital_df.sort_values(by=['latitude', 'longitude'], ascending=[True, True])
        
        closest_hospitals = pd.DataFrame()
        for hospital_type in hospital_df["hospital_type"].unique():
            type_df = hospital_df[hospital_df["hospital_type"] == hospital_type]
            type_df = type_df.sort_values(by=['longitude_sub', 'latitude_sub'], ascending=[True, True])
            logging.info(type_df)
            type_df = type_df[:2]
            closest = type_df.drop(labels=["longitude_sub", "latitude_sub"], axis=1)
            closest_hospitals = pd.concat([closest_hospitals, closest])

        hospital_list = closest_hospitals.values.tolist()
        # logging.info(hospital_list)

        hospital_data = []
        hospital_data.append(str(id))
        hospital_data.append(a)
        hospital_data.append(b)
        hospital_data.append("병원")
        for i in range(len(hospital_list)):
            hospital_data.append(hospital_list[i][0])
            hospital_data.append(hospital_list[i][1])
            hospital_data.append(hospital_list[i][2])
            hospital_data.append(hospital_list[i][3])
        hospital_transformed_data.append(hospital_data)

    context["ti"].xcom_push(key="hospital_transformed_data_2", value=hospital_transformed_data)


def hospital_transform_3(**context):
    hospital_extracted_list = context["ti"].xcom_pull(key="hospital_extracted_data")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_3")

    hospital_transformed_data = []
    hospital_df = pd.DataFrame(hospital_extracted_list, columns=['hospital_name', 'hospital_type', 'longitude', 'latitude'])
    address_df = pd.DataFrame(address_list, columns=['id', 'longitude', 'latitude'])

    for index_1, row_1 in address_df.iterrows():
        a = float(row_1['longitude'])
        b = float(row_1['latitude'])
        id = str(row_1['id'])
        hospital_df["longitude_sub"] = hospital_df["longitude"] - a
        hospital_df["latitude_sub"] = hospital_df["latitude"] - b
        hospital_df["longitude_sub"] = hospital_df["longitude_sub"].abs()
        hospital_df["latitude_sub"] = hospital_df["latitude_sub"].abs()

        hospital_df = hospital_df.sort_values(by=['latitude', 'longitude'], ascending=[True, True])
        
        closest_hospitals = pd.DataFrame()
        for hospital_type in hospital_df["hospital_type"].unique():
            type_df = hospital_df[hospital_df["hospital_type"] == hospital_type]
            type_df = type_df.sort_values(by=['longitude_sub', 'latitude_sub'], ascending=[True, True])
            logging.info(type_df)
            type_df = type_df[:2]
            closest = type_df.drop(labels=["longitude_sub", "latitude_sub"], axis=1)
            closest_hospitals = pd.concat([closest_hospitals, closest])

        hospital_list = closest_hospitals.values.tolist()
        # logging.info(hospital_list)

        hospital_data = []
        hospital_data.append(str(id))
        hospital_data.append(a)
        hospital_data.append(b)
        hospital_data.append("병원")
        for i in range(len(hospital_list)):
            hospital_data.append(hospital_list[i][0])
            hospital_data.append(hospital_list[i][1])
            hospital_data.append(hospital_list[i][2])
            hospital_data.append(hospital_list[i][3])
        hospital_transformed_data.append(hospital_data)

    context["ti"].xcom_push(key="hospital_transformed_data_3", value=hospital_transformed_data)


def hospital_transform_4(**context):
    hospital_extracted_list = context["ti"].xcom_pull(key="hospital_extracted_data")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_4")

    hospital_transformed_data = []
    hospital_df = pd.DataFrame(hospital_extracted_list, columns=['hospital_name', 'hospital_type', 'longitude', 'latitude'])
    address_df = pd.DataFrame(address_list, columns=['id', 'longitude', 'latitude'])

    for index_1, row_1 in address_df.iterrows():
        a = float(row_1['longitude'])
        b = float(row_1['latitude'])
        id = str(row_1['id'])
        hospital_df["longitude_sub"] = hospital_df["longitude"] - a
        hospital_df["latitude_sub"] = hospital_df["latitude"] - b
        hospital_df["longitude_sub"] = hospital_df["longitude_sub"].abs()
        hospital_df["latitude_sub"] = hospital_df["latitude_sub"].abs()

        hospital_df = hospital_df.sort_values(by=['latitude', 'longitude'], ascending=[True, True])
        
        closest_hospitals = pd.DataFrame()
        for hospital_type in hospital_df["hospital_type"].unique():
            type_df = hospital_df[hospital_df["hospital_type"] == hospital_type]
            type_df = type_df.sort_values(by=['longitude_sub', 'latitude_sub'], ascending=[True, True])
            logging.info(type_df)
            type_df = type_df[:2]
            closest = type_df.drop(labels=["longitude_sub", "latitude_sub"], axis=1)
            closest_hospitals = pd.concat([closest_hospitals, closest])

        hospital_list = closest_hospitals.values.tolist()
        # logging.info(hospital_list)

        hospital_data = []
        hospital_data.append(str(id))
        hospital_data.append(a)
        hospital_data.append(b)
        hospital_data.append("병원")
        for i in range(len(hospital_list)):
            hospital_data.append(hospital_list[i][0])
            hospital_data.append(hospital_list[i][1])
            hospital_data.append(hospital_list[i][2])
            hospital_data.append(hospital_list[i][3])
        hospital_transformed_data.append(hospital_data)

    context["ti"].xcom_push(key="hospital_transformed_data_4", value=hospital_transformed_data)


def hospital_transform_5(**context):
    hospital_extracted_list = context["ti"].xcom_pull(key="hospital_extracted_data")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_5")

    hospital_transformed_data = []
    hospital_df = pd.DataFrame(hospital_extracted_list, columns=['hospital_name', 'hospital_type', 'longitude', 'latitude'])
    address_df = pd.DataFrame(address_list, columns=['id', 'longitude', 'latitude'])

    for index_1, row_1 in address_df.iterrows():
        a = float(row_1['longitude'])
        b = float(row_1['latitude'])
        id = str(row_1['id'])
        hospital_df["longitude_sub"] = hospital_df["longitude"] - a
        hospital_df["latitude_sub"] = hospital_df["latitude"] - b
        hospital_df["longitude_sub"] = hospital_df["longitude_sub"].abs()
        hospital_df["latitude_sub"] = hospital_df["latitude_sub"].abs()

        hospital_df = hospital_df.sort_values(by=['latitude', 'longitude'], ascending=[True, True])
        
        closest_hospitals = pd.DataFrame()
        for hospital_type in hospital_df["hospital_type"].unique():
            type_df = hospital_df[hospital_df["hospital_type"] == hospital_type]
            type_df = type_df.sort_values(by=['longitude_sub', 'latitude_sub'], ascending=[True, True])
            logging.info(type_df)
            type_df = type_df[:2]
            closest = type_df.drop(labels=["longitude_sub", "latitude_sub"], axis=1)
            closest_hospitals = pd.concat([closest_hospitals, closest])

        hospital_list = closest_hospitals.values.tolist()
        # logging.info(hospital_list)

        hospital_data = []
        hospital_data.append(str(id))
        hospital_data.append(a)
        hospital_data.append(b)
        hospital_data.append("병원")
        for i in range(len(hospital_list)):
            hospital_data.append(hospital_list[i][0])
            hospital_data.append(hospital_list[i][1])
            hospital_data.append(hospital_list[i][2])
            hospital_data.append(hospital_list[i][3])
        hospital_transformed_data.append(hospital_data)

    context["ti"].xcom_push(key="hospital_transformed_data_5", value=hospital_transformed_data)


def hospital_transform_6(**context):
    hospital_extracted_list = context["ti"].xcom_pull(key="hospital_extracted_data")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_6")

    hospital_transformed_data = []
    hospital_df = pd.DataFrame(hospital_extracted_list, columns=['hospital_name', 'hospital_type', 'longitude', 'latitude'])
    address_df = pd.DataFrame(address_list, columns=['id', 'longitude', 'latitude'])

    for index_1, row_1 in address_df.iterrows():
        a = float(row_1['longitude'])
        b = float(row_1['latitude'])
        id = str(row_1['id'])
        hospital_df["longitude_sub"] = hospital_df["longitude"] - a
        hospital_df["latitude_sub"] = hospital_df["latitude"] - b
        hospital_df["longitude_sub"] = hospital_df["longitude_sub"].abs()
        hospital_df["latitude_sub"] = hospital_df["latitude_sub"].abs()

        hospital_df = hospital_df.sort_values(by=['latitude', 'longitude'], ascending=[True, True])
        
        closest_hospitals = pd.DataFrame()
        for hospital_type in hospital_df["hospital_type"].unique():
            type_df = hospital_df[hospital_df["hospital_type"] == hospital_type]
            type_df = type_df.sort_values(by=['longitude_sub', 'latitude_sub'], ascending=[True, True])
            logging.info(type_df)
            type_df = type_df[:2]
            closest = type_df.drop(labels=["longitude_sub", "latitude_sub"], axis=1)
            closest_hospitals = pd.concat([closest_hospitals, closest])

        hospital_list = closest_hospitals.values.tolist()
        # logging.info(hospital_list)

        hospital_data = []
        hospital_data.append(str(id))
        hospital_data.append(a)
        hospital_data.append(b)
        hospital_data.append("병원")
        for i in range(len(hospital_list)):
            hospital_data.append(hospital_list[i][0])
            hospital_data.append(hospital_list[i][1])
            hospital_data.append(hospital_list[i][2])
            hospital_data.append(hospital_list[i][3])
        hospital_transformed_data.append(hospital_data)

    context["ti"].xcom_push(key="hospital_transformed_data_6", value=hospital_transformed_data)


def combineAllData(**context):
    transformed_data_1 = context["ti"].xcom_pull(key="hospital_transformed_data_1")
    transformed_data_2 = context["ti"].xcom_pull(key="hospital_transformed_data_2")
    transformed_data_3 = context["ti"].xcom_pull(key="hospital_transformed_data_3")
    transformed_data_4 = context["ti"].xcom_pull(key="hospital_transformed_data_4")
    transformed_data_5 = context["ti"].xcom_pull(key="hospital_transformed_data_5")
    transformed_data_6 = context["ti"].xcom_pull(key="hospital_transformed_data_6")

    combine_data = transformed_data_1 + transformed_data_2 + transformed_data_3 + transformed_data_4 + transformed_data_5 + transformed_data_6

    context["ti"].xcom_push(key="combined_data", value=combine_data)


def loadToCSV(**context):
   data_list = context["ti"].xcom_pull(key="combined_data")
   df = pd.DataFrame(data_list)
   df.to_csv(FILE_NAME, index=False, header=False)


# CSV 파일 S3로 업로드하는 함수
def uploadToS3():
    bucket_name = BUCKET_NAME
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename=f'./{FILE_NAME}',
        key=f'data/{FILE_NAME}', 
        bucket_name= bucket_name, 
        replace=True
   )
    

# S3에서 Redshift로 COPY해서 적재하는 함수
def loadToRedshift(autocommit=True):
   redshift_hook = PostgresHook(postgres_conn_id='rs_conn')
   conn = redshift_hook.get_conn()
   conn.autocommit = autocommit
   cursor = conn.cursor()

    # Redshift용 COPY 명령문
   copy_query = f"""
   COPY raw_data.hospital_near_estate
   FROM 's3://{BUCKET_NAME}/data/{FILE_NAME}'
   IAM_ROLE '{IAM_ROLE}'
   CSV
   IGNOREHEADER 1;
   """
    
   cursor.execute(copy_query)
   conn.commit()
   cursor.close()

CreatehospitalTable = PostgresOperator(
    task_id = "create_hospital_table",
    postgres_conn_id ='rs_conn',
    sql = CREATE_QUERY,
    dag = dag
)

GetDataCount = PythonOperator(
    task_id = "get_data_count",
    python_callable=getDataCount,
    dag = dag
)

DecideNextTask = BranchPythonOperator(
    task_id = "decide_next_task",
    python_callable=decideNextTask,
    dag = dag
)

ExtractAllEstate = PythonOperator(
    task_id = "extract_all_estate",
    python_callable=extractAllEstate,
    dag = dag
)

ExtractUniqueEstate = PythonOperator(
    task_id = "extract_unique_estate",
    python_callable=extractUniqueEstate,
    dag = dag
)

DummyJoin = EmptyOperator(
    task_id='dummy_join',
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

TransformEstateData = PythonOperator(
    task_id = "transform_estate_data",
    python_callable=transformEstateData,
    dag = dag
)

hospital_extract_task = PythonOperator(
    task_id = "hospital_extract",
    python_callable=hospital_extract,
    dag=dag
)

hospital_transform_task_1 = PythonOperator(
    task_id = "hospital_transform_1",
    python_callable=hospital_transform_1,
    dag=dag
)

hospital_transform_task_2 = PythonOperator(
    task_id = "hospital_transform_2",
    python_callable=hospital_transform_2,
    dag=dag
)

hospital_transform_task_3 = PythonOperator(
    task_id = "hospital_transform_3",
    python_callable=hospital_transform_3,
    dag=dag
)

hospital_transform_task_4 = PythonOperator(
    task_id = "hospital_transform_4",
    python_callable=hospital_transform_4,
    dag=dag
)

hospital_transform_task_5 = PythonOperator(
    task_id = "hospital_transform_5",
    python_callable=hospital_transform_5,
    dag=dag
)

hospital_transform_task_6 = PythonOperator(
    task_id = "hospital_transform_6",
    python_callable=hospital_transform_6,
    dag=dag
)

combine_all_data_task = PythonOperator(
    task_id = "combine_all_data",
    python_callable=combineAllData,
    dag=dag
)

load_to_csv_task = PythonOperator(
    task_id = "load_to_csv",
    python_callable=loadToCSV,
    dag=dag
)

upload_to_S3_task = PythonOperator(
    task_id = "upload_to_s3",
    python_callable=uploadToS3,
    dag=dag
)

load_to_redshift_task = PythonOperator(
    task_id = "load_to_redshift",
    python_callable=loadToRedshift,
    dag=dag
)

CreatehospitalTable >> GetDataCount >> DecideNextTask >> ExtractUniqueEstate
DecideNextTask >> ExtractAllEstate >> DummyJoin
DecideNextTask >> ExtractUniqueEstate >> DummyJoin
DummyJoin >> TransformEstateData >> hospital_extract_task >> hospital_transform_task_1 >> hospital_transform_task_2 >>  hospital_transform_task_3 >> hospital_transform_task_4 >> hospital_transform_task_5 >> hospital_transform_task_6
hospital_transform_task_6 >> combine_all_data_task
combine_all_data_task >> load_to_csv_task >> upload_to_S3_task >> load_to_redshift_task