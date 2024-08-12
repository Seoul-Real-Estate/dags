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

URL = Variable.get('kakao_api_url')
API_KEY_1 = Variable.get('kakao_api_key_1')
API_KEY_2 = Variable.get('kakao_api_key_2')
API_KEY_3 = Variable.get('kakao_api_key_3')
API_KEY_4 = Variable.get('kakao_api_key_4')
RADIUS = Variable.get('kakao_radius')
SIZE = Variable.get('kakao_size')
SORT = Variable.get('kakao_sort')
FILE_NAME = "infra_near_estate.csv"
BUCKET_NAME = Variable.get('bucket_name')
IAM_ROLE = Variable.get('aws_iam_role')

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='estateInfraDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    catchup=False
)

# raw_data.infra_near_estate 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS raw_data.infra_near_estate (
    estate_id VARCHAR(255),
    x FLOAT,
    y FLOAT,
    mart_name_1 VARCHAR(500),
    mart_distance_1 INT,
    mart_category_1 VARCHAR(500),
    mart_address_1 VARCHAR(500),
    mart_x_1 FLOAT,
    mart_y_1 FLOAT,
    mart_name_2 VARCHAR(500),
    mart_distance_2 INT,
    mart_category_2 VARCHAR(500),
    mart_address_2 VARCHAR(500),
    mart_x_2 FLOAT,
    mart_y_2 FLOAT,
    mart_name_3 VARCHAR(500),
    mart_distance_3 INT,
    mart_category_3 VARCHAR(500),
    mart_address_3 VARCHAR(500),
    mart_x_3 FLOAT,
    mart_y_3 FLOAT,
    convenience_name_1 VARCHAR(500),
    convenience_distance_1 INT,
    convenience_category_1 VARCHAR(500),
    convenience_address_1 VARCHAR(500),
    convenience_x_1 FLOAT,
    convenience_y_1 FLOAT,
    convenience_name_2 VARCHAR(500),
    convenience_distance_2 INT,
    convenience_category_2 VARCHAR(500),
    convenience_address_2 VARCHAR(500),
    convenience_x_2 FLOAT,
    convenience_y_2 FLOAT,
    convenience_name_3 VARCHAR(500),
    convenience_distance_3 INT,
    convenience_category_3 VARCHAR(500),
    convenience_address_3 VARCHAR(500),
    convenience_x_3 FLOAT,
    convenience_y_3 FLOAT,
    preschool_name_1 VARCHAR(500),
    preschool_distance_1 INT,
    preschool_category_1 VARCHAR(500),
    preschool_address_1 VARCHAR(500),
    preschool_x_1 FLOAT,
    preschool_y_1 FLOAT,
    preschool_name_2 VARCHAR(500),
    preschool_distance_2 INT,
    preschool_category_2 VARCHAR(500),
    preschool_address_2 VARCHAR(500),
    preschool_x_2 FLOAT,
    preschool_y_2 FLOAT,
    preschool_name_3 VARCHAR(500),
    preschool_distance_3 INT,
    preschool_category_3 VARCHAR(500),
    preschool_address_3 VARCHAR(500),
    preschool_x_3 FLOAT,
    preschool_y_3 FLOAT,
    school_name_1 VARCHAR(500),
    school_distance_1 INT,
    school_category_1 VARCHAR(500),
    school_address_1 VARCHAR(500),
    school_x_1 FLOAT,
    school_y_1 FLOAT,
    school_name_2 VARCHAR(500),
    school_distance_2 INT,
    school_category_2 VARCHAR(500),
    school_address_2 VARCHAR(500),
    school_x_2 FLOAT,
    school_y_2 FLOAT,
    school_name_3 VARCHAR(500),
    school_distance_3 INT,
    school_category_3 VARCHAR(500),
    school_address_3 VARCHAR(500),
    school_x_3 FLOAT,
    school_y_3 FLOAT,
    gas_station_name_1 VARCHAR(500),
    gas_station_distance_1 INT,
    gas_station_category_1 VARCHAR(500),
    gas_station_address_1 VARCHAR(500),
    gas_station_x_1 FLOAT,
    gas_station_y_1 FLOAT,
    gas_station_name_2 VARCHAR(500),
    gas_station_distance_2 INT,
    gas_station_category_2 VARCHAR(500),
    gas_station_address_2 VARCHAR(500),
    gas_station_x_2 FLOAT,
    gas_station_y_2 FLOAT,
    gas_station_name_3 VARCHAR(500),
    gas_station_distance_3 INT,
    gas_station_category_3 VARCHAR(500),
    gas_station_address_3 VARCHAR(500),
    gas_station_x_3 FLOAT,
    gas_station_y_3 FLOAT,
    subway_name_1 VARCHAR(500),
    subway_distance_1 INT,
    subway_category_1 VARCHAR(500),
    subway_address_1 VARCHAR(500),
    subway_x_1 FLOAT,
    subway_y_1 FLOAT,
    subway_name_2 VARCHAR(500),
    subway_distance_2 INT,
    subway_category_2 VARCHAR(500),
    subway_address_2 VARCHAR(500),
    subway_x_2 FLOAT,
    subway_y_2 FLOAT,
    subway_name_3 VARCHAR(500),
    subway_distance_3 INT,
    subway_category_3 VARCHAR(500),
    subway_address_3 VARCHAR(500),
    subway_x_3 FLOAT,
    subway_y_3 FLOAT,
    bank_name_1 VARCHAR(500),
    bank_distance_1 INT,
    bank_category_1 VARCHAR(500),
    bank_address_1 VARCHAR(500),
    bank_x_1 FLOAT,
    bank_y_1 FLOAT,
    bank_name_2 VARCHAR(500),
    bank_distance_2 INT,
    bank_category_2 VARCHAR(500),
    bank_address_2 VARCHAR(500),
    bank_x_2 FLOAT,
    bank_y_2 FLOAT,
    bank_name_3 VARCHAR(500),
    bank_distance_3 INT,
    bank_category_3 VARCHAR(500),
    bank_address_3 VARCHAR(500),
    bank_x_3 FLOAT,
    bank_y_3 FLOAT,
    culture_name_1 VARCHAR(500),
    culture_distance_1 INT,
    culture_category_1 VARCHAR(500),
    culture_address_1 VARCHAR(500),
    culture_x_1 FLOAT,
    culture_y_1 FLOAT,
    culture_name_2 VARCHAR(500),
    culture_distance_2 INT,
    culture_category_2 VARCHAR(500),
    culture_address_2 VARCHAR(500),
    culture_x_2 FLOAT,
    culture_y_2 FLOAT,
    culture_name_3 VARCHAR(500),
    culture_distance_3 INT,
    culture_category_3 VARCHAR(500),
    culture_address_3 VARCHAR(500),
    culture_x_3 FLOAT,
    culture_y_3 FLOAT,
    public_name_1 VARCHAR(500),
    public_distance_1 INT,
    public_category_1 VARCHAR(500),
    public_address_1 VARCHAR(500),
    public_x_1 FLOAT,
    public_y_1 FLOAT,
    public_name_2 VARCHAR(500),
    public_distance_2 INT,
    public_category_2 VARCHAR(500),
    public_address_2 VARCHAR(500),
    public_x_2 FLOAT,
    public_y_2 FLOAT,
    public_name_3 VARCHAR(500),
    public_distance_3 INT,
    public_category_3 VARCHAR(500),
    public_address_3 VARCHAR(500),
    public_x_3 FLOAT,
    public_y_3 FLOAT,
    hospital_name_1 VARCHAR(500),
    hospital_distance_1 INT,
    hospital_category_1 VARCHAR(500),
    hospital_address_1 VARCHAR(500),
    hospital_x_1 FLOAT,
    hospital_y_1 FLOAT,
    hospital_name_2 VARCHAR(500),
    hospital_distance_2 INT,
    hospital_category_2 VARCHAR(500),
    hospital_address_2 VARCHAR(500),
    hospital_x_2 FLOAT,
    hospital_y_2 FLOAT,
    hospital_name_3 VARCHAR(500),
    hospital_distance_3 INT,
    hospital_category_3 VARCHAR(500),
    hospital_address_3 VARCHAR(500),
    hospital_x_3 FLOAT,
    hospital_y_3 FLOAT,
    pharmacy_name_1 VARCHAR(500),
    pharmacy_distance_1 INT,
    pharmacy_category_1 VARCHAR(500),
    pharmacy_address_1 VARCHAR(500),
    pharmacy_x_1 FLOAT,
    pharmacy_y_1 FLOAT,
    pharmacy_name_2 VARCHAR(500),
    pharmacy_distance_2 INT,
    pharmacy_category_2 VARCHAR(500),
    pharmacy_address_2 VARCHAR(500),
    pharmacy_x_2 FLOAT,
    pharmacy_y_2 FLOAT,
    pharmacy_name_3 VARCHAR(500),
    pharmacy_distance_3 INT,
    pharmacy_category_3 VARCHAR(500),
    pharmacy_address_3 VARCHAR(500),
    pharmacy_x_3 FLOAT,
    pharmacy_y_3 FLOAT
);
"""

def getDataCount(**context):
    redshift_hook = RedshiftSQLHook(redshift_conn_id='rs_conn')
    
    sql = """
    SELECT COUNT(*) FROM raw_data.infra_near_estate;
    """
    
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()        

    context["ti"].xcom_push(key="data_cnt", value=rows[0][0])


def decideNextTask(**context):
    cnt = context['ti'].xcom_pull(key="data_cnt")

    if int(cnt) == 0:
        return 'extract_allEstate'
    else:
        return 'extract_uniqueEstate'

CreateInfraTable = PostgresOperator(
    task_id = "create_estateInfra_table",
    postgres_conn_id ='rs_conn',
    sql = CREATE_QUERY,
    dag = dag
)


def extractAllEstate(**context):
    logging.info("estractAllEstate")

    redshift_hook = RedshiftSQLHook(redshift_conn_id='rs_conn')
    
    sql = """
        SELECT articleno, longitude, latitude FROM raw_data.naver_real_estate;
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
    SELECT A.articleno, A.longitude, A.latitude FROM raw_data.naver_real_estate AS A
    LEFT JOIN raw_data.infra_near_estate AS B
    ON A.articleno = B.estate_id 
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
    logging.info(f"data_length : {len(data)} length : {length}")
    data_number = length // 4
    end = 0
    for i in range(3):
        start = i * data_number
        end = (i + 1) * data_number
        logging.info(f"start : {start} - end : {end}")
        arr = data[start: end]
        context["ti"].xcom_push(key=f"estate_transform_data_{i+1}", value=arr)
    
    arr = data[end: length]
    logging.info(f"start : {end} - end : {length}")
    context["ti"].xcom_push(key=f"estate_transform_data_4", value=arr)


def infraExtract_1(**context):
    address_list = context["ti"].xcom_pull(key="estate_transform_data_1")
    logging.info(len(address_list))
    extracted_list = []
    category = {'MT1': '마트', 'CS2': '편의점', 'PS3': '유치원', 'SC4':'학교', 'OL7':'주유소', 'SW8': '지하철역', 'BK9': '은행', 'CT1':'문화시설', 'PO3': '공공기관', 'HP8':'병원', 'PN9':'약국'}
    for address in address_list:
        data_list = []
        for key, value in category.items():
            url = URL

            headers = {
                'Authorization': f"KakaoAK {API_KEY_1}"
            }
            params = {
                'category_group_code' : key,
                'y' : address[2],
                'x' : address[1],
                'radius' : RADIUS,
                'size': SIZE,
                'sort': SORT
            }

            response = requests.get(url, headers=headers, params=params)
            data = json.loads(response.text)
            logging.info(f"key : {API_KEY_1}")
            if len(data["documents"]) == 0:
                temp = []
                for i in range(3):
                    temp.append({'address_name': '', 'category_group_code': key, 'category_group_name': value, 'category_name': '', 'distance': '0', 'id': '', 'phone': '', 'place_name': '', 'place_url': '', 'road_address_name': '', 'x': '0.0', 'y': '0.0'})
                    data["documents"] = temp
            data_list.extend(data["documents"])
        extracted_list.append(data_list)  
    context["ti"].xcom_push(key="infra_extracted_data_1", value=extracted_list)


def infraExtract_2(**context):
    address_list = context["ti"].xcom_pull(key="estate_transform_data_2")
    extracted_list = []
    category = {'MT1': '마트', 'CS2': '편의점', 'PS3': '유치원', 'SC4':'학교', 'OL7':'주유소', 'SW8': '지하철역', 'BK9': '은행', 'CT1':'문화시설', 'PO3': '공공기관', 'HP8':'병원', 'PN9':'약국'}
    for address in address_list:
        data_list = []
        for key, value in category.items():
            url = URL
            headers = {
                'Authorization': f"KakaoAK {API_KEY_2}"
            }
            params = {
                'category_group_code' : key,
                'y' : address[2],
                'x' : address[1],
                'radius' : RADIUS,
                'size': SIZE,
                'sort': SORT
            }

            response = requests.get(url, headers=headers, params=params)
            data = json.loads(response.text)
            logging.info(data)
            if len(data["documents"]) == 0:
                temp = []
                for i in range(3):
                    temp.append({'address_name': '', 'category_group_code': key, 'category_group_name': value, 'category_name': '', 'distance': '0', 'id': '', 'phone': '', 'place_name': '', 'place_url': '', 'road_address_name': '', 'x': '0.0', 'y': '0.0'})
                    data["documents"] = temp
            data_list.extend(data["documents"])
        extracted_list.append(data_list)  
    context["ti"].xcom_push(key="infra_extracted_data_2", value=extracted_list)


def infraExtract_3(**context):
    address_list = context["ti"].xcom_pull(key="estate_transform_data_3")
    extracted_list = []
    category = {'MT1': '마트', 'CS2': '편의점', 'PS3': '유치원', 'SC4':'학교', 'OL7':'주유소', 'SW8': '지하철역', 'BK9': '은행', 'CT1':'문화시설', 'PO3': '공공기관', 'HP8':'병원', 'PN9':'약국'}
    for address in address_list:
        data_list = []
        for key, value in category.items():
            url = URL

            headers = {
                'Authorization': f"KakaoAK {API_KEY_3}"
            }
            params = {
                'category_group_code' : key,
                'y' : address[2],
                'x' : address[1],
                'radius' : RADIUS,
                'size': SIZE,
                'sort': SORT
            }

            response = requests.get(url, headers=headers, params=params)
            data = json.loads(response.text)
            if len(data["documents"]) == 0:
                temp = []
                for i in range(3):
                    temp.append({'address_name': '', 'category_group_code': key, 'category_group_name': value, 'category_name': '', 'distance': '0', 'id': '', 'phone': '', 'place_name': '', 'place_url': '', 'road_address_name': '', 'x': '0.0', 'y': '0.0'})
                    data["documents"] = temp
            data_list.extend(data["documents"])
        extracted_list.append(data_list)  
    context["ti"].xcom_push(key="infra_extracted_data_3", value=extracted_list)


def infraExtract_4(**context):
    address_list = context["ti"].xcom_pull(key="estate_transform_data_4")
    extracted_list = []
    category = {'MT1': '마트', 'CS2': '편의점', 'PS3': '유치원', 'SC4':'학교', 'OL7':'주유소', 'SW8': '지하철역', 'BK9': '은행', 'CT1':'문화시설', 'PO3': '공공기관', 'HP8':'병원', 'PN9':'약국'}
    for address in address_list:
        data_list = []
        for key, value in category.items():
            url = URL

            headers = {
                'Authorization': f"KakaoAK {API_KEY_4}"
            }
            params = {
                'category_group_code' : key,
                'y' : address[2],
                'x' : address[1],
                'radius' : RADIUS,
                'size': SIZE,
                'sort': SORT
            }

            response = requests.get(url, headers=headers, params=params)
            data = json.loads(response.text)
            if len(data["documents"]) == 0:
                temp = []
                for i in range(3):
                    temp.append({'address_name': '', 'category_group_code': key, 'category_group_name': value, 'category_name': '', 'distance': '0', 'id': '', 'phone': '', 'place_name': '', 'place_url': '', 'road_address_name': '', 'x': '0.0', 'y': '0.0'})
                    data["documents"] = temp
            data_list.extend(data["documents"])
        extracted_list.append(data_list)  
    context["ti"].xcom_push(key="infra_extracted_data_4", value=extracted_list)


def infraTransform_1(**context):
    infra_extracted_list = context["ti"].xcom_pull(key="infra_extracted_data_1")
    address_list = context["ti"].xcom_pull(key="estate_transform_data_1")

    category_dict = {'MT1': 'mart', 'CS2': 'convenience', 'PS3': 'preschool', 'SC4':'school', 'OL7':'gas_station', 'SW8': 'subway', 'BK9': 'bank', 'CT1':'culture', 'PO3': 'public', 'HP8':'hospital', 'PN9':'pharmacy'}

    trans_list = []
    for ex_idx in range(len(infra_extracted_list)):
        trans_dict = {}
        extract = infra_extracted_list[ex_idx]
        logging.info(extract)
        trans_dict[f'estate_id'] = str(address_list[ex_idx][0])
        trans_dict[f'x'] = float(address_list[ex_idx][1])
        trans_dict[f'y'] = float(address_list[ex_idx][2])
        for extract_data in extract:
            code = extract_data['category_group_code']
            for idx in range(3):
                trans_dict[f'{category_dict[code]}_name_{idx+1}'] = extract_data['place_name']
                trans_dict[f'{category_dict[code]}_distance_{idx+1}'] = int(extract_data['distance'])
                trans_dict[f'{category_dict[code]}_category_{idx+1}'] = extract_data['category_group_name']
                trans_dict[f'{category_dict[code]}_address_{idx+1}'] = extract_data['road_address_name']
                trans_dict[f'{category_dict[code]}_x_{idx+1}'] = float(extract_data['x'])
                trans_dict[f'{category_dict[code]}_y_{idx+1}'] = float(extract_data['y'])

        trans_list.append(trans_dict)
   
    context["ti"].xcom_push(key="infra_transformed_data_1", value=trans_list)


def infraTransform_2(**context):
   infra_extracted_list = context["ti"].xcom_pull(key="infra_extracted_data_2")
   address_list = context["ti"].xcom_pull(key="estate_transform_data_2")

   category_dict = {'MT1': 'mart', 'CS2': 'convenience', 'PS3': 'preschool', 'SC4':'school', 'OL7':'gas_station', 'SW8': 'subway', 'BK9': 'bank', 'CT1':'culture', 'PO3': 'public', 'HP8':'hospital', 'PN9':'pharmacy'}

   trans_list = []
   for ex_idx in range(len(infra_extracted_list)):
      trans_dict = {}
      extract = infra_extracted_list[ex_idx]
      logging.info(extract)
      trans_dict[f'estate_id'] = str(address_list[ex_idx][0])
      trans_dict[f'x'] = float(address_list[ex_idx][1])
      trans_dict[f'y'] = float(address_list[ex_idx][2])
      for extract_data in extract:
         code = extract_data['category_group_code']
         for idx in range(3):
            trans_dict[f'{category_dict[code]}_name_{idx+1}'] = extract_data['place_name']
            trans_dict[f'{category_dict[code]}_distance_{idx+1}'] = int(extract_data['distance'])
            trans_dict[f'{category_dict[code]}_category_{idx+1}'] = extract_data['category_group_name']
            trans_dict[f'{category_dict[code]}_address_{idx+1}'] = extract_data['road_address_name']
            trans_dict[f'{category_dict[code]}_x_{idx+1}'] = float(extract_data['x'])
            trans_dict[f'{category_dict[code]}_y_{idx+1}'] = float(extract_data['y'])

      trans_list.append(trans_dict)
   
   context["ti"].xcom_push(key="infra_transformed_data_2", value=trans_list)


def infraTransform_3(**context):
   infra_extracted_list = context["ti"].xcom_pull(key="infra_extracted_data_3")
   address_list = context["ti"].xcom_pull(key="estate_transform_data_3")

   category_dict = {'MT1': 'mart', 'CS2': 'convenience', 'PS3': 'preschool', 'SC4':'school', 'OL7':'gas_station', 'SW8': 'subway', 'BK9': 'bank', 'CT1':'culture', 'PO3': 'public', 'HP8':'hospital', 'PN9':'pharmacy'}

   trans_list = []
   for ex_idx in range(len(infra_extracted_list)):
      trans_dict = {}
      extract = infra_extracted_list[ex_idx]
      logging.info(extract)
      trans_dict[f'estate_id'] = str(address_list[ex_idx][0])
      trans_dict[f'x'] = float(address_list[ex_idx][1])
      trans_dict[f'y'] = float(address_list[ex_idx][2])
      for extract_data in extract:
         code = extract_data['category_group_code']
         for idx in range(3):
            trans_dict[f'{category_dict[code]}_name_{idx+1}'] = extract_data['place_name']
            trans_dict[f'{category_dict[code]}_distance_{idx+1}'] = int(extract_data['distance'])
            trans_dict[f'{category_dict[code]}_category_{idx+1}'] = extract_data['category_group_name']
            trans_dict[f'{category_dict[code]}_address_{idx+1}'] = extract_data['road_address_name']
            trans_dict[f'{category_dict[code]}_x_{idx+1}'] = float(extract_data['x'])
            trans_dict[f'{category_dict[code]}_y_{idx+1}'] = float(extract_data['y'])

      trans_list.append(trans_dict)
   
   context["ti"].xcom_push(key="infra_transformed_data_3", value=trans_list)


def infraTransform_4(**context):
   infra_extracted_list = context["ti"].xcom_pull(key="infra_extracted_data_4")
   address_list = context["ti"].xcom_pull(key="estate_transform_data_4")

   category_dict = {'MT1': 'mart', 'CS2': 'convenience', 'PS3': 'preschool', 'SC4':'school', 'OL7':'gas_station', 'SW8': 'subway', 'BK9': 'bank', 'CT1':'culture', 'PO3': 'public', 'HP8':'hospital', 'PN9':'pharmacy'}

   trans_list = []
   for ex_idx in range(len(infra_extracted_list)):
      trans_dict = {}
      extract = infra_extracted_list[ex_idx]
      logging.info(extract)
      trans_dict[f'estate_id'] = str(address_list[ex_idx][0])
      trans_dict[f'x'] = float(address_list[ex_idx][1])
      trans_dict[f'y'] = float(address_list[ex_idx][2])
      for extract_data in extract:
         code = extract_data['category_group_code']
         for idx in range(3):
            trans_dict[f'{category_dict[code]}_name_{idx+1}'] = extract_data['place_name']
            trans_dict[f'{category_dict[code]}_distance_{idx+1}'] = int(extract_data['distance'])
            trans_dict[f'{category_dict[code]}_category_{idx+1}'] = extract_data['category_group_name']
            trans_dict[f'{category_dict[code]}_address_{idx+1}'] = extract_data['road_address_name']
            trans_dict[f'{category_dict[code]}_x_{idx+1}'] = float(extract_data['x'])
            trans_dict[f'{category_dict[code]}_y_{idx+1}'] = float(extract_data['y'])

      trans_list.append(trans_dict)
   
   context["ti"].xcom_push(key="infra_transformed_data_4", value=trans_list)


def combineAllData(**context):
    transformed_data_1 = context["ti"].xcom_pull(key="infra_transformed_data_1")
    transformed_data_2 = context["ti"].xcom_pull(key="infra_transformed_data_2")
    transformed_data_3 = context["ti"].xcom_pull(key="infra_transformed_data_3")
    transformed_data_4 = context["ti"].xcom_pull(key="infra_transformed_data_4")

    combine_data = transformed_data_1 + transformed_data_2 + transformed_data_3 + transformed_data_4

    context["ti"].xcom_push(key="combined_data", value=combine_data)


def loadToCSV(**context):
   data_list = context["ti"].xcom_pull(key="combined_data")
   df = pd.DataFrame(data_list)
   df.to_csv(FILE_NAME, index=False)


def uploadToS3():
    bucket_name = BUCKET_NAME
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename=f'./{FILE_NAME}',
        key=f'data/{FILE_NAME}', 
        bucket_name= bucket_name, 
        replace=True
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
    task_id = "extract_allEstate",
    python_callable=extractAllEstate,
    dag = dag
)

ExtractUniqueEstate = PythonOperator(
    task_id = "extract_uniqueEstate",
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

InfraExtract_1 = PythonOperator(
    task_id = "infra_extract_1",
    python_callable=infraExtract_1,
    dag=dag
)

InfraExtract_2 = PythonOperator(
    task_id = "infra_extract_2",
    python_callable=infraExtract_2,
    dag=dag
)

InfraExtract_3 = PythonOperator(
    task_id = "infra_extract_3",
    python_callable=infraExtract_3,
    dag=dag
)

InfraExtract_4 = PythonOperator(
    task_id = "infra_extract_4",
    python_callable=infraExtract_4,
    dag=dag
)

InfraTransform_1 = PythonOperator(
    task_id = "infra_transform_1",
    python_callable=infraTransform_1,
    dag=dag
)

InfraTransform_2 = PythonOperator(
    task_id = "infra_transform_2",
    python_callable=infraTransform_2,
    dag=dag
)

InfraTransform_3 = PythonOperator(
    task_id = "infra_transform_3",
    python_callable=infraTransform_3,
    dag=dag
)

InfraTransform_4 = PythonOperator(
    task_id = "infra_transform_4",
    python_callable=infraTransform_4,
    dag=dag
)

CombineAllData = PythonOperator(
    task_id = "combine_all_data",
    python_callable=combineAllData,
    dag=dag
)

LoadToCSV = PythonOperator(
    task_id = "load_to_csv",
    python_callable=loadToCSV,
    dag=dag
)

UploadToS3 = PythonOperator(
    task_id = "upload_to_s3",
    python_callable=uploadToS3,
    dag=dag
)
