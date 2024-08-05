from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
import requests
import logging
import xml.etree.ElementTree as ET
import pendulum
import json
import pandas as pd
import math

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='infra_testDAG',
    start_date=datetime(2024, 7, 16, tzinfo=kst),
    schedule=None,
    catchup=False
)

# seoul_subway 테이블 생성 쿼리
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS analytics.infra_test (
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
    pharmacy_y_3 FLOAT,
    bus_station_id_1 INT,
    bus_station_name_1 VARCHAR(500),
    bus_station_distance_1 INT,
    bus_station_x_1 FLOAT,
    bus_station_y_1 FLOAT,
    bus_station_id_2 INT,
    bus_station_name_2 VARCHAR(500),
    bus_station_distance_2 INT,
    bus_station_x_2 FLOAT,
    bus_station_y_2 FLOAT,
    bus_station_id_3 INT,
    bus_station_name_3 VARCHAR(500),
    bus_station_distance_3 INT,
    bus_station_x_3 FLOAT,
    bus_station_y_3 FLOAT
);
"""

def getAddress(**context):
   arr = [['127.065915414443', '37.4996157705058'], ['127.011582043737', '37.5078602234885'], ['127.044671518265', '37.5392578661678']]
   context["ti"].xcom_push(key="address", value=arr)

def bus_extract(**context):
   address_list = context["ti"].xcom_pull(key="address")
   extracted_data = []
   for a in address_list:
      url = 'https://api.odsay.com/v1/api/pointBusStation?'
      params = {
            'apiKey': 'Jr2EJ129r3z2s2BjjD9SLbm+JMvyBdBFRbh9OtoaXHA',
            'lang' : '0',
            'x' : f'{a[0]}',
            'y' : f'{a[1]}',
            'radius' : '500',
        }
      response = requests.get(url, params=params)
      extracted_data.append(response.text)
   context["ti"].xcom_push(key="bus_extracted_data", value=extracted_data)

def infra_extract(**context):
   address_list = context["ti"].xcom_pull(key="address")
   extracted_list = []
   category = {'MT1': '마트', 'CS2': '편의점', 'PS3': '유치원', 'SC4':'학교', 'OL7':'주유소', 'SW8': '지하철역', 'BK9': '은행', 'CT1':'문화시설', 'PO3': '공공기관', 'HP8':'병원', 'PN9':'약국'}
   for address in address_list:
      data_list = []
      for key, value in category.items():
         url = "https://dapi.kakao.com/v2/local/search/category.json"

         headers = {
            'Authorization': 'KakaoAK 102ca12f155306b8ed8f3eb72b24185d'
         }
         params = {
            'category_group_code' : key,
            'y' : address[1],
            'x' : address[0],
            'radius' : '20000',
            'size':'3',
            'sort':'distance'
         }

         response = requests.get(url, headers=headers, params=params)
         data = json.loads(response.text)
         logging.info(data)
         if data["documents"] == []:
            temp = []
            for i in range(3):
               temp.append({'address_name': '', 'category_group_code': key, 'category_group_name': value, 'category_name': '', 'distance': '0', 'id': '', 'phone': '', 'place_name': '', 'place_url': '', 'road_address_name': '', 'x': '0.0', 'y': '0.0'})
            data["documents"] = temp
         data_list.extend(data["documents"])
      extracted_list.append(data_list)  
   context["ti"].xcom_push(key="infra_extracted_data", value=extracted_list)


def infra_transform(**context):
   infra_extracted_list = context["ti"].xcom_pull(key="infra_extracted_data")
   address_list = context["ti"].xcom_pull(key="address")

   category_dict = {'MT1': 'mart', 'CS2': 'convenience', 'PS3': 'preschool', 'SC4':'school', 'OL7':'gas_station', 'SW8': 'subway', 'BK9': 'bank', 'CT1':'culture', 'PO3': 'public', 'HP8':'hospital', 'PN9':'pharmacy'}
   attributes = ['name', 'distance', 'category', 'address', 'x', 'y']

   trans_list = []
   for ex_idx in range(len(infra_extracted_list)):
      trans_dict = {}
      extract = infra_extracted_list[ex_idx]
      logging.info(extract)
      trans_dict[f'x'] = float(address_list[ex_idx][0])
      trans_dict[f'y'] = float(address_list[ex_idx][1])
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
   
   context["ti"].xcom_push(key="infra_transformed_data", value=trans_list)

def bus_transform(**context):
   bus_extracted_list = context["ti"].xcom_pull(key="bus_extracted_data")
   address_list = context["ti"].xcom_pull(key="address")

   trans_list = []
   for idx in range(len(bus_extracted_list)):
      address = address_list[idx]
      bus_trans_dict = {}
      json_data = json.loads(bus_extracted_list[idx])
      for element in json_data['result']['lane']:
         for i in range(3):
            station_x = float(element["x"])
            station_y = float(element["y"])
            house_x = float(address[0])
            house_y = float(address[1])

            distance = math.ceil(math.sqrt((abs(station_x - house_x)**2) + (abs(station_y - house_y)**2)))

            bus_trans_dict[f'bus_station_id_{i+1}'] = element["stationID"]
            bus_trans_dict[f'bus_station_name_{i+1}'] = element["stationName"]
            bus_trans_dict[f'bus_station_distance_{i+1}'] = distance
            bus_trans_dict[f'bus_station_x_{i+1}'] = element["x"]
            bus_trans_dict[f'bus_station_y_{i+1}'] = element["y"]
         trans_list.append(bus_trans_dict)

   context["ti"].xcom_push(key="bus_transformed_data", value=trans_list)

def combineAllData(**context):
   address_list = context["ti"].xcom_pull(key="address")
   bus_data_list = context["ti"].xcom_pull(key="bus_transformed_data")
   infra_data_list = context["ti"].xcom_pull(key="infra_transformed_data")

   result = []
   for i in range(len(address_list)):
      r = infra_data_list[i]
      r.update(bus_data_list[i])
      result.append(r)
   
   context["ti"].xcom_push(key="combined_data", value=result)

def loadToCSV(**context):
   data_list = context["ti"].xcom_pull(key="combined_data")
   df = pd.DataFrame(data_list)
   df.to_csv("infra_test.csv", index=False)

# CSV 파일 S3로 업로드하는 함수
def upload_to_S3():
    bucket_name = 'team-ariel-2-data'
    hook = S3Hook(aws_conn_id='S3_conn')
    hook.load_file(
        filename='./infra_test.csv',
        key='data/infra_test.csv', 
        bucket_name= bucket_name, 
        replace=True
    )

# S3에서 Redshift로 COPY해서 적재하는 함수
def load_to_redshift():
   redshift_hook = PostgresHook(postgres_conn_id='rs_conn')
   conn = redshift_hook.get_conn()
   cursor = conn.cursor()

   truncate_query = "TRUNCATE TABLE analytics.infra_test;"
   cursor.execute(truncate_query)
   conn.commit()

    # Redshift용 COPY 명령문
   copy_query = f"""
   COPY analytics.infra_test
   FROM 's3://team-ariel-2-data/data/infra_test.csv'
   IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
   CSV
   IGNOREHEADER 1;
   """
    
   cursor.execute(copy_query)
   conn.commit()
   cursor.close()

createInfraTable = PostgresOperator(
    task_id = "create_infra_table",
    postgres_conn_id='rs_conn',
    sql=CREATE_QUERY,
    dag=dag
)

getAddressData = PythonOperator(
   task_id = "get_address",
   python_callable=getAddress,
   dag=dag
)

# 지하철 데이터 추출하는 Task
DataExtract = PythonOperator(
    task_id = "infra_extract",
    python_callable=infra_extract,
    dag=dag
)

# 버스 데이터 추출하는 Task
BusExtract = PythonOperator(
    task_id = "bus_extract",
    python_callable=bus_extract,
    dag=dag
)

# 추출한 인프라 데이터 변환하는 Task
DataTransform = PythonOperator(
    task_id = "infra_transform",
    python_callable=infra_transform,
    dag=dag
)

# 추출한 버스 데이터 변환하는 Task
BusTransform = PythonOperator(
    task_id = "bus_transform",
    python_callable=bus_transform,
    dag=dag
)

CombineAllDataList= PythonOperator(
    task_id = "combine_all_data",
    python_callable=combineAllData,
    dag=dag
)

dataLoadToCSV = PythonOperator(
    task_id = "infra_load_to_csv",
    python_callable=loadToCSV,
    dag=dag
)

# CSV 파일 S3에 저장하는 Task
upload_data_to_S3 = PythonOperator(
    task_id = "upload_to_S3",
    python_callable=upload_to_S3,
    
    dag = dag
)

# S3에서 Redshift로 COPY해서 적재하는 Task
load_data_to_redshift = PythonOperator(
    task_id = "load_to_redshift",
    python_callable=load_to_redshift
)

createInfraTable >> getAddressData >> [DataExtract, BusExtract]
DataExtract >> DataTransform
BusExtract >> BusTransform
[DataTransform, BusTransform] >> CombineAllDataList >> dataLoadToCSV >> upload_data_to_S3 >> load_data_to_redshift