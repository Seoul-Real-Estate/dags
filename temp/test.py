import boto3
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

# S3 클라이언트 생성
# s3 = boto3.client('s3')

# def read_csv_from_s3(bucket_name, object_key):
#     """
#     S3에서 CSV 파일을 읽어와 pandas DataFrame으로 변환하는 함수
#     :param bucket_name: S3 버킷 이름
#     :param object_key: S3 객체 키 (파일 경로)
#     :return: pandas DataFrame
#     """
#     try:
#         # S3에서 파일을 가져오기
#         response = s3.get_object(Bucket=bucket_name, Key=object_key)
        
#         # 파일 내용을 문자열로 읽어오기
#         csv_string = response['Body'].read().decode('utf-8')
        
#         # pandas의 read_csv 메소드로 DataFrame으로 변환
#         df = pd.read_csv(StringIO(csv_string))
        
#         return df
#     except Exception as e:
#         print(f"Error reading {object_key} from bucket {bucket_name}: {e}")
#         return None

# # S3 버킷 이름과 객체 키 (파일 경로)를 지정
# bucket_name = 'team-ariel-2-data'
# object_key = 'data/apartment_rent_202407_20240725.csv'

# # CSV 파일을 읽어 DataFrame으로 변환
# df = read_csv_from_s3(bucket_name, object_key)

# # DataFrame 출력
# if df is not None:
#     print(df.head())

import requests

url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiTrade'
params ={'serviceKey' : '서비스키', 'LAWD_CD' : '11110', 'DEAL_YMD' : '201512' }

response = requests.get(url, params=params).content
soup = BeautifulSoup(response, 'lxml-xml')
rows = soup.find_all('item')



key = "ZU3VKtV/cyVYqylBKhohTTGbwd5/hq0d4YDqWyHz9kODNZMljBKxidxikPm6J4uY7MTEGHQfT4+FuK/UEGmNkQ=="

url = 'http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade'
apartment_trade_origin_cols = ['dealYear','dealMonth', 'dealDay', 'umdNm', 'aptNm',  'excluUseAr', 'dealAmount', 'buildYear',
                         'jibun', 'sggCd', 'floor', 'cdealDay', 'dealingGbn', 'rgstDate']
apartent_trade_cosl = ['year', 'month', 'day', '법정동명', '건물명', '전용면적', '물건금액', '건축년도', '지번', '지역코드', '층', '취소일', '신고구분', '등기일자']

rentcols_fin = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세구분', '임대면적', '보증금', '임대료',
            '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
rentcols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '전월세구분', '임대면적', '보증금', '임대료',
            '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
tradecols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '물건금액', '전용면적', '토지면적', '권리구분',
            '취소일', '건축년도', '건물용도', '신고구분', '등기일자']
tradecols_fin = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액', '전용면적', '토지면적', '권리구분',
            '취소일', '건축년도', '건물용도', '신고구분', '등기일자']

re = realestate('20240804', '202408')
result = re.data_extract(url, key, apartment_trade_origin_cols, apartent_trade_cosl, re.apartment_trade_list)
# print(result)
df = re.apartment_trade_preprocessing(result, tradecols)

# fdf = re.compare_add_latlon(pd.DataFrame([], columns= df.columns), df.head(5), rentcols_fin)
# print(fdf)

# print(re.get_lat_lng("서울특별시 용산구 이태원동"))