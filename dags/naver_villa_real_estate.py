from datetime import datetime, timedelta, date
from io import StringIO

import pandas as pd
import requests
import logging
from airflow.decorators import task, dag, task_group
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

default_args = {
    'owner': 'yong',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

iam_role = Variable.get('aws_iam_role')


def get_redshift_connection(autocommit=True):
    hook = RedshiftSQLHook(redshift_conn_id='redshift_dev')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


# file_name 파일이 S3 bucket_name 에 있는가?
def is_check_s3_file_exists(bucket_name, file_name):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    key = 'data/' + file_name
    return s3_hook.check_for_key(key=key, bucket_name=bucket_name)


# s3의 data/ 폴더의 file_name CSV 파일 가져오기
def get_csv_from_s3(bucket_name, file_name):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    key = 'data/' + file_name
    return pd.read_csv(s3_hook.get_key(key=key, bucket_name=bucket_name).get()['Body'])


# 법정동코드(cortarNo) 지역의 동네 리스트를 데이터프레임으로 가져오기
def get_region_info(url, headers, cortarNo):
    url = f"{url}{cortarNo}"
    res = requests.get(url, headers=headers)
    _json = res.json()
    return pd.DataFrame(_json["regionList"])


# 데이터프레임을 S3에 CSV파일로 업로드
def upload_to_s3(bucket_name, file_name, data_frame):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    csv_buffer = StringIO()
    key = 'data/' + file_name
    data_frame.to_csv(csv_buffer, index=False)
    s3_hook.load_string(string_data=csv_buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True)


# 법정동 코드로 해당 동네 빌라/주택 매물 리스트 검색
def get_villa_real_estate(headers, cortarNo):
    url = "https://new.land.naver.com/api/articles"
    params = {
        "cortarNo": int(cortarNo),
        "order": "rank",
        "realEstateType": "JWJT:HOJT:VL:DDDGG:SGJT",
        "tradeType": "",
        "tag": " ::::::::",
        "rentPriceMin": " 0",
        "rentPriceMax": " 900000000",
        "priceMin": " 0",
        "priceMax": " 900000000",
        "areaMin": " 0",
        "areaMax": " 900000000",
        "oldBuildYears": "",
        "recentlyBuildYears": "",
        "minHouseHoldCount": "",
        "maxHouseHoldCount": "",
        "showArticle": " false",
        "sameAddressGroup": " false",
        "minMaintenanceCost": "",
        "maxMaintenanceCost": "",
        "priceType": " RETAIL",
        "directions": "",
        "page": " 1",
        "articleState": ""
    }
    headers = headers.copy()
    naver_token = Variable.get('naver_token')
    headers.update({"Authorization": f"{naver_token}"})
    res = requests.get(url, params=params, headers=headers)
    _json = res.json()
    return pd.DataFrame(_json["articleList"])


# 매물 기본키(articleNo) 가져오기
def get_naver_real_estate_pk(schema):
    cur = get_redshift_connection()
    cur.execute(f'SELECT articleNo FROM {schema}.naver_real_estate')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


# 공인중개사 기본키(realtorId) 가져오기
def get_naver_realtor_pk(schema):
    cur = get_redshift_connection()
    cur.execute(f'SELECT realtorid FROM {schema}.naver_realtor')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


def get_today_file_name(file_name):
    today = date.today()
    return f'{today}_{file_name}'


# 빌라 매물 상세정보 가져오기 (공인중개사 포함)
def get_villa_real_estate_detail(headers, articleNo):
    url = f"https://new.land.naver.com/api/articles/{articleNo}"
    params = {"complexNo": ""}
    headers = headers.copy()
    naver_token = Variable.get('naver_token')
    headers.update({"Authorization": f"{naver_token}"})
    return requests.get(url, params=params, headers=headers).json()


# 숫자 변환 시 에러가 발생하는 값은 NaN으로 대체
def clean_numeric_column(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')


@dag(
    default_args=default_args,
    description="네이버부동산 빌라/주택 데이터 수집 및 적재 DAG",
    schedule_interval='0 1 * * *',
    start_date=datetime(2024, 8, 4),
    catchup=False,
    tags=['daily', 'real_estate', 'naver', 'villa']
)
def naver_villa_real_estate():
    schema = 'raw_data'
    headers = {
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }
    seoul_cortarNo = 1100000000
    bucket_name = 'team-ariel-2-data'
    si_gun_gu_file_name = 'si_gun_gu.csv'
    eup_myeon_dong_file_name = 'eup_myeon_dong.csv'
    villa_file_name = 'naver_villa_real_estate.csv'
    realtor_file_name = 'naver_villa_realtor.csv'
    base_url = "https://new.land.naver.com/api/regions/list?cortarNo="

    @task_group(group_id='process_region')
    def process_region():
        # 1. 시/군/구 정보 검색 API
        @task
        def process_si_gun_gu():
            if is_check_s3_file_exists(bucket_name, si_gun_gu_file_name):
                return

            df = get_region_info(base_url, headers, seoul_cortarNo)
            upload_to_s3(bucket_name, si_gun_gu_file_name, df)

        # 2. 읍/면/동 정보 검색 API
        @task
        def process_eup_myeon_dong():
            if is_check_s3_file_exists(bucket_name, eup_myeon_dong_file_name):
                return

            eup_myeon_dong_df_list = []
            si_gun_gu_df = get_csv_from_s3(bucket_name, si_gun_gu_file_name)
            for cortarNo in si_gun_gu_df['cortarNo']:
                eup_myeon_dong_df = get_region_info(base_url, headers, cortarNo)
                eup_myeon_dong_df_list.append(eup_myeon_dong_df)

            eup_myeon_dong_df = pd.concat(eup_myeon_dong_df_list, ignore_index=True)
            upload_to_s3(bucket_name, eup_myeon_dong_file_name, eup_myeon_dong_df)

        process_si_gun_gu() >> process_eup_myeon_dong()

    @task_group(group_id='process_villa')
    def process_villa():
        # 3. 동마다 빌라/주택 매물 리스트 검색
        @task
        def process_villa_real_estate():
            dong_df = get_csv_from_s3(bucket_name, eup_myeon_dong_file_name)
            villa_df_list = []

            for cortarNo in dong_df['cortarNo']:
                villa_df = get_villa_real_estate(headers, cortarNo)
                villa_df_list.append(villa_df)

            villa_df = pd.concat(villa_df_list, ignore_index=True)
            today_villa_file_name = get_today_file_name(villa_file_name)
            upload_to_s3(bucket_name, today_villa_file_name, villa_df)

        # 4. 빌라/주택 새로운 매물만 추출
        @task.short_circuit
        def extract_new_villa():
            today_villa_file_name = get_today_file_name(villa_file_name)
            villa_df = get_csv_from_s3(bucket_name, today_villa_file_name)
            article_numbers = get_naver_real_estate_pk(schema)
            new_villa_df = villa_df[~villa_df['articleNo'].isin(article_numbers)]

            if new_villa_df.empty:
                return False

            upload_to_s3(bucket_name, today_villa_file_name, new_villa_df)
            return True

        # 5. 빌라/주택 상세정보 검색
        @task
        def process_villa_real_estate_detail():
            today_villa_file_name = get_today_file_name(villa_file_name)
            villa_df = get_csv_from_s3(bucket_name, today_villa_file_name)
            realtor_infos = []

            # 필요한 열 추가
            necessary_columns = [
                'complexNo', 'articleName', 'detailAddress', 'exposureAddress', 'parkingCount', 'parkingPossibleYN',
                'principalUse', 'useApproveYmd', 'cortarNo', 'roomCount', 'bathroomCount', 'detailDescription',
                'rentPrice', 'warrantPrice', 'dealPrice', 'hoNm', 'correspondingFloorCount', 'totalFloorCount',
                'address', 'roadAddress', 'etcAddress', 'roomFacilityCodes', 'roomFacilities', 'buildingFacilityCodes',
                'buildingFacilities', 'roofTopYN', 'exposeStartYMD', 'exposeEndYMD', 'walkingTimeToNearSubway',
                'heatMethodTypeCode', 'heatMethodTypeName', 'heatFuelTypeCode', 'heatFuelTypeName', 'supply_area',
                'exclusive_area'
            ]

            for column in necessary_columns:
                if column not in villa_df.columns:
                    villa_df[column] = None

            for idx, row in villa_df.iterrows():
                villa_detail_info = get_villa_real_estate_detail(headers, row['articleNo'])
                articleDetail = villa_detail_info.get('articleDetail', {})
                articlePrice = villa_detail_info.get('articlePrice', {})
                articleFloor = villa_detail_info.get('articleFloor', {})
                landPrice = villa_detail_info.get('landPrice', {})
                articleRealtor = villa_detail_info.get('articleRealtor', {})
                articleOneroom = villa_detail_info.get('articleOneroom', {})
                articleFacility = villa_detail_info.get('articleFacility', {})

                villa_df.loc[idx, 'articleName'] = articleDetail.get('articleName')
                villa_df.loc[idx, 'detailAddress'] = articleDetail.get('detailAddress')
                villa_df.loc[idx, 'exposureAddress'] = articleDetail.get('exposureAddress')
                villa_df.loc[idx, 'parkingCount'] = articleDetail.get('parkingCount')
                villa_df.loc[idx, 'parkingPossibleYN'] = articleDetail.get('parkingPossibleYN')
                villa_df.loc[idx, 'principalUse'] = articleDetail.get('principalUse')
                villa_df.loc[idx, 'useApproveYmd'] = articleFacility.get('buildingUseAprvYmd')
                villa_df.loc[idx, 'cortarNo'] = articleDetail.get('cortarNo')
                villa_df.loc[idx, 'roomCount'] = articleDetail.get('roomCount')
                villa_df.loc[idx, 'bathroomCount'] = articleDetail.get('bathroomCount')
                villa_df.loc[idx, 'detailDescription'] = articleDetail.get('detailDescription', '')
                villa_df.loc[idx, 'rentPrice'] = articlePrice.get('rentPrice')
                villa_df.loc[idx, 'warrantPrice'] = articlePrice.get('warrantPrice')
                villa_df.loc[idx, 'dealPrice'] = articlePrice.get('dealPrice')
                villa_df.loc[idx, 'hoNm'] = landPrice.get('hoNm')
                villa_df.loc[idx, 'correspondingFloorCount'] = articleFloor.get('correspondingFloorCount')
                villa_df.loc[idx, 'totalFloorCount'] = articleFloor.get('totalFloorCount')
                villa_df.loc[idx, 'etcAddress'] = articleDetail.get('etcAddress')

                if articleOneroom:
                    villa_df.at[idx, 'roomFacilityCodes'] = articleOneroom.get('roomFacilityCodes', '')
                    villa_df.at[idx, 'roomFacilities'] = articleOneroom.get('roomFacilities', [])
                    villa_df.at[idx, 'buildingFacilityCodes'] = articleOneroom.get('buildingFacilityCodes', '')
                    villa_df.at[idx, 'buildingFacilities'] = articleOneroom.get('buildingFacilities', [])
                    villa_df.loc[idx, 'roofTopYN'] = articleOneroom.get('roofTopYN', '')
                else:
                    villa_df.at[idx, 'roomFacilityCodes'] = articleFacility.get('lifeFacilityList', '')
                    villa_df.at[idx, 'roomFacilities'] = articleFacility.get('lifeFacilities', [])
                    villa_df.at[idx, 'buildingFacilityCodes'] = articleFacility.get('securityFacilityList', '')
                    villa_df.at[idx, 'buildingFacilities'] = articleFacility.get('securityFacilities', [])

                villa_df.loc[idx, 'exposeStartYMD'] = articleDetail.get('exposeStartYMD')
                villa_df.loc[idx, 'exposeEndYMD'] = articleDetail.get('exposeEndYMD')
                villa_df.loc[idx, 'walkingTimeToNearSubway'] = articleDetail.get('walkingTimeToNearSubway')
                villa_df.loc[idx, 'heatMethodTypeCode'] = articleFacility.get('heatMethodTypeCode', '')
                villa_df.loc[idx, 'heatMethodTypeName'] = articleFacility.get('heatMethodTypeName', '')
                villa_df.loc[idx, 'heatFuelTypeCode'] = articleFacility.get('heatFuelTypeCode', '')
                villa_df.loc[idx, 'heatFuelTypeName'] = articleFacility.get('heatFuelTypeName', '')

                # 공인중개사 값 추가
                realtor_infos.append(articleRealtor)

            upload_to_s3(bucket_name, today_villa_file_name, villa_df)

            realtor_df = pd.DataFrame(realtor_infos)
            realtor_df = realtor_df.drop_duplicates(subset='realtorId')
            today_realtor_file_name = get_today_file_name(realtor_file_name)
            upload_to_s3(bucket_name, today_realtor_file_name, realtor_df)

        process_villa_real_estate() >> extract_new_villa() >> process_villa_real_estate_detail()

    # 6. 빌라/주택 전처리
    @task
    def transform_villa_real_estate():
        today_villa_file_name = get_today_file_name(villa_file_name)
        villa_df = get_csv_from_s3(bucket_name, today_villa_file_name)

        # 정수형 필드에 문자열이 올 경우 NaN 변환
        clean_numeric_column(villa_df, 'roomCount')
        clean_numeric_column(villa_df, 'bathroomCount')
        clean_numeric_column(villa_df, 'dealPrice')
        clean_numeric_column(villa_df, 'warrantPrice')
        clean_numeric_column(villa_df, 'rentPrice')

        villa_df.loc[:, 'rentPrc'] = villa_df['rentPrc'].fillna(0)
        villa_df['supply_area'] = villa_df['area1'].fillna(0).astype(int)
        villa_df['exclusive_area'] = villa_df['area2'].fillna(0).astype(int)
        villa_df['parkingCount'] = villa_df['parkingCount'].fillna(0).astype(int)
        villa_df['roomCount'] = villa_df['roomCount'].fillna(0).astype(int)
        villa_df['bathroomCount'] = villa_df['bathroomCount'].fillna(0).astype(int)
        villa_df['walkingTimeToNearSubway'] = villa_df['walkingTimeToNearSubway'].fillna(0).astype(int)
        villa_df['created_at'] = datetime.now()
        villa_df['updated_at'] = datetime.now()

        # 컬럼 순서 맞추기
        desired_columns = ['articleNo', 'realtorId', 'complexNo', 'articleName', 'realEstateTypeName', 'tradeTypeName',
                           'floorInfo', 'correspondingFloorCount', 'totalFloorCount', 'dealOrWarrantPrc', 'rentPrc',
                           'dealPrice', 'warrantPrice', 'rentPrice', 'supply_area', 'exclusive_area', 'direction',
                           'articleConfirmYmd', 'articleFeatureDesc', 'detailDescription', 'tagList', 'latitude',
                           'longitude', 'detailAddress', 'exposureAddress', 'address', 'roadAddress', 'etcAddress',
                           'buildingName', 'hoNm', 'cortarNo', 'parkingCount', 'parkingPossibleYN', 'principalUse',
                           'roomCount', 'bathroomCount', 'roomFacilityCodes', 'roomFacilities', 'buildingFacilityCodes',
                           'buildingFacilities', 'roofTopYN', 'useApproveYmd', 'exposeStartYMD', 'exposeEndYMD',
                           'walkingTimeToNearSubway', 'heatMethodTypeCode', 'heatMethodTypeName', 'heatFuelTypeCode',
                           'heatFuelTypeName', 'created_at', 'updated_at',
                           ]
        villa_df = villa_df[desired_columns]
        today_transform_villa_file_name = get_today_file_name('transform_' + villa_file_name)
        upload_to_s3(bucket_name, today_transform_villa_file_name, villa_df)

    # 7. 빌라/주택 redshift 적재
    @task
    def load_to_redshift_villa():
        today_transform_villa_file_name = get_today_file_name('transform_' + villa_file_name)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {schema}.naver_real_estate
                    FROM 's3://team-ariel-2-data/data/{today_transform_villa_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV 
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {schema}.naver_real_estate')

    # 8. 새로운 공인중개사 추출
    @task.short_circuit
    def extract_new_realtor():
        today_realtor_file_name = get_today_file_name(realtor_file_name)
        realtor_df = get_csv_from_s3(bucket_name, today_realtor_file_name)
        realtor_ids = get_naver_realtor_pk(schema)
        new_realtor_df = realtor_df[~realtor_df['realtorId'].isin(realtor_ids)]
        if new_realtor_df.empty:
            return False

        upload_to_s3(bucket_name, today_realtor_file_name, new_realtor_df)
        return True

    # 9. 공인중개사 전처리
    @task
    def transform_realtor():
        today_realtor_file_name = get_today_file_name(realtor_file_name)
        realtor_df = get_csv_from_s3(bucket_name, today_realtor_file_name)
        desired_columns = ['realtorId', 'realtorName', 'representativeName', 'address', 'establishRegistrationNo',
                           'dealCount', 'leaseCount', 'rentCount', 'latitude', 'longitude', 'representativeTelNo',
                           'cellPhoneNo', 'cortarNo']
        realtor_df = realtor_df[desired_columns]
        realtor_df = realtor_df[realtor_df['realtorId'].notna()]
        realtor_df['dealCount'] = realtor_df['dealCount'].fillna(0).astype(int)
        realtor_df['leaseCount'] = realtor_df['leaseCount'].fillna(0).astype(int)
        realtor_df['rentCount'] = realtor_df['rentCount'].fillna(0).astype(int)
        realtor_df['created_at'] = datetime.now()
        realtor_df['updated_at'] = datetime.now()
        today_transform_realtor_file_name = get_today_file_name('transform_' + realtor_file_name)
        upload_to_s3(bucket_name, today_transform_realtor_file_name, realtor_df)

    # 10. 공인중개사 redshift 적재
    @task
    def load_to_redshift_realtor():
        today_transform_realtor_file_name = get_today_file_name('transform_' + realtor_file_name)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {schema}.naver_realtor
                    FROM 's3://team-ariel-2-data/data/{today_transform_realtor_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV 
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {schema}.naver_realtor')

    region = process_region()
    process_villa = process_villa()
    transform_villa_task = transform_villa_real_estate()
    load_to_redshift_villa_task = load_to_redshift_villa()
    extract_new_realtor_task = extract_new_realtor()
    transform_realtor = transform_realtor()
    load_to_redshift_realtor_task = load_to_redshift_realtor()

    region >> process_villa
    process_villa >> transform_villa_task >> load_to_redshift_villa_task
    process_villa >> extract_new_realtor_task >> transform_realtor >> load_to_redshift_realtor_task


naver_villa_real_estate = naver_villa_real_estate()
