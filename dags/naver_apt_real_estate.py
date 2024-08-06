from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
import logging
from airflow.decorators import task, dag
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


# 법정동코드(cortarNo) 지역의 동네 리스트를 데이터프레임으로 가져오기
def get_region_info(base_url, base_headers, cortarNo):
    url = f"{base_url}{cortarNo}"
    res = requests.get(url, headers=base_headers)
    _json = res.json()
    return pd.DataFrame(_json["regionList"])


# 데이터프레임을 S3에 CSV파일로 업로드
def upload_to_s3(bucket_name, file_name, data_frame):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    csv_buffer = StringIO()
    key = 'data/' + file_name
    data_frame.to_csv(csv_buffer, index=False)
    s3_hook.load_string(string_data=csv_buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True)


# file_name 파일이 S3 bucket_name 에 있는가?
def is_check_s3_file_exists(bucket_name, file_name):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    key = 'data/' + file_name
    return s3_hook.check_for_key(key=key, bucket_name=bucket_name)


# 아파트 단지 리스트 가져오기
def get_apt_complex_info(headers, cortarNo):
    complex_url = f"https://new.land.naver.com/api/regions/complexes?cortarNo={cortarNo}&realEstateType=OPST:APT&order="
    res = requests.get(complex_url, headers=headers)
    return pd.DataFrame(res.json()["complexList"])


# 아파트 단지 상세정보 가져오기
def get_apt_complex_detail_info(headers, complexNo):
    url = f"https://new.land.naver.com/api/complexes/{complexNo}"
    params = {"sameAddressGroup": "false"}
    headers = headers.copy()
    headers.update({
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3MjA1MjY2NTEsImV4cCI6MTcyMDUzNzQ1MX0.UOUEwVmXdYY-wjtxtmkYQaEOcgU0SBaCaSk5bC7ihpE"})

    try:
        res = requests.get(url, params=params, headers=headers)
        res.raise_for_status()
        return res.json().get("complexDetail")
    except requests.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        logging.error(f"Response Content: {res.text}")
    except Exception as err:
        logging.error(f"Other error occurred: {err}")


# 아파트 단지의 매물 가져오기
def get_apt_real_estate_info(base_headers, complexNo):
    url = f"https://new.land.naver.com/api/articles/complex/{complexNo}"
    params = {
        "realEstateType": "APT",
        "tag": "%3A%3A%3A%3A%3A%3A%3A%3",
        "rentPriceMin": 0,
        "rentPriceMax": 900000000,
        "priceMin": 0,
        "priceMax": 900000000,
        "areaMin": 0,
        "areaMax": 900000000,
        "showArticle": "false",
        "sameAddressGroup": "false",
        "priceType": "RETAIL",
        "page": 1,
        "complexNo": int(complexNo),
        "type": "list",
        "order": "rank",
        "tradeType": "",
        "oldBuildYears": "",
        "recentlyBuildYears": "",
        "minHouseHoldCount": "",
        "maxHouseHoldCount": "",
        "minMaintenanceCost": "",
        "maxMaintenanceCost": "",
        "directions": "",
        "buildingNos": "",
        "areaNos": ""
    }
    headers = base_headers.copy()
    headers.update({
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3MjA1MjY2NTEsImV4cCI6MTcyMDUzNzQ1MX0.UOUEwVmXdYY-wjtxtmkYQaEOcgU0SBaCaSk5bC7ihpE"})
    res = requests.get(url, params=params, headers=headers)
    _json = res.json()
    return pd.DataFrame(_json["articleList"])


# 아파트 단지 기본키(complexNo) 가져오기
def get_complex_primary_keys(schema):
    cur = get_redshift_connection()
    cur.execute(f'SELECT complexNo FROM {schema}.naver_complex')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


# 매물 기본키(articleNo) 가져오기
def get_real_estate_primary_keys(schema):
    cur = get_redshift_connection()
    cur.execute(f'SELECT articleNo FROM {schema}.naver_real_estate')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


# 아파트 매물 상세정보 가져오기 (공인중개사 정보 포함)
def get_real_estate_detail_info(base_headers, articleNo):
    url = f"https://new.land.naver.com/api/articles/{articleNo}"
    params = {"complexNo": ""}
    headers = base_headers.copy()
    headers.update({
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE3MjA1MjY2NTEsImV4cCI6MTcyMDUzNzQ1MX0.UOUEwVmXdYY-wjtxtmkYQaEOcgU0SBaCaSk5bC7ihpE"})
    return requests.get(url, params=params, headers=headers).json()


# 숫자 변환 시 에러가 발생하는 값은 NaN으로 대체
def clean_numeric_column(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')


# s3의 data/ 폴더의 file_name CSV 파일 가져오기
def get_csv_from_s3(bucket_name, file_name):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    key = 'data/' + file_name
    return pd.read_csv(s3_hook.get_key(key=key, bucket_name=bucket_name).get()['Body'])


def get_today_file_name(file_name):
    today = datetime.now().strftime('%Y-%m-%d')
    return f'{today}_{file_name}'


@dag(
    default_args=default_args,
    description="네이버부동산 아파트/오피스텔 데이터 수집 및 적재 DAG",
    schedule_interval='0 1 * * *',
    start_date=datetime(2024, 8, 4),
    catchup=False,
    tags=['daily', 'real_estate', 'naver', 'apt']
)
def naver_apt_real_estate():
    schema = 'raw_data'
    seoul_cortarNo = 1100000000
    bucket_name = 'team-ariel-2-data'
    si_gun_gu_file_name = 'si_gun_gu.csv'
    eup_myeon_dong_file_name = 'eup_myeon_dong.csv'
    complex_file_name = 'naver_complex.csv'
    real_estate_file_name = 'naver_real_estate.csv'
    realtor_file_name = 'naver_realtor.csv'
    base_url = "https://new.land.naver.com/api/regions/list?cortarNo="
    base_headers = {
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }

    # 1. 시/군/구 정보 검색 API
    @task
    def process_si_gun_gu():
        is_check = is_check_s3_file_exists(bucket_name, si_gun_gu_file_name)

        if not is_check:
            df = get_region_info(base_url, base_headers, seoul_cortarNo)
            upload_to_s3(bucket_name, si_gun_gu_file_name, df)

    # 2. 읍/면/동 정보 검색 API
    @task
    def process_eup_myeon_dong():
        is_check = is_check_s3_file_exists(bucket_name, eup_myeon_dong_file_name)

        if not is_check:
            eup_myeon_dong_df_list = []
            si_gun_gu_df = get_csv_from_s3(bucket_name, si_gun_gu_file_name)

            for cortarNo in si_gun_gu_df['cortarNo']:
                eup_myeon_dong_df = get_region_info(base_url, base_headers, cortarNo)
                eup_myeon_dong_df_list.append(eup_myeon_dong_df)

            eup_myeon_dong_df = pd.concat(eup_myeon_dong_df_list, ignore_index=True)
            upload_to_s3(bucket_name, eup_myeon_dong_file_name, eup_myeon_dong_df)

    # 3. 동마다 아파트 단지 리스트 검색
    @task.branch
    def process_apt_complex():
        dong_df = get_csv_from_s3(bucket_name, eup_myeon_dong_file_name)
        complex_df_list = []

        for cortarNo in dong_df['cortarNo']:
            complex_df = get_apt_complex_info(base_headers, cortarNo)
            complex_df_list.append(complex_df)

        complex_df = pd.concat(complex_df_list, ignore_index=True)
        complex_df.drop(
            columns=['detailAddress', 'totalBuildingCount', 'highFloor', 'lowFloor', 'cortarAddress', 'dealCount',
                     'leaseCount', 'rentCount', 'shortTermRentCount'], inplace=True)

        # redshift 조회 후 비교하여 새로운 아파트 단지만 추출
        complex_numbers = get_complex_primary_keys(schema)
        new_complex_df = complex_df[~complex_df['complexNo'].isin(complex_numbers)]

        # 새로운 아파트 단지가 없으면 상세정보 검색 테스크 생략
        if new_complex_df.empty:
            return 'skip_task'

        # 새로운 아파트 단지가 있을 때
        today_complex_file_name = get_today_file_name(complex_file_name)
        upload_to_s3(bucket_name, today_complex_file_name, new_complex_df)
        return 'process_apt_complex_detail'

    # 4. 단지마다 아파트 단지 상세정보 검색
    @task
    def process_apt_complex_detail():
        today_complex_file_name = get_today_file_name(complex_file_name)
        complex_df = get_csv_from_s3(bucket_name, today_complex_file_name)

        # 상세 정보에서 필요한 데이터 key
        keys = [
            "address", "detailAddress", "roadAddress", "roadAddressPrefix", "maxSupplyArea", "minSupplyArea",
            "parkingPossibleCount", "parkingCountByHousehold", "constructionCompanyName", "heatMethodTypeCode",
            "heatFuelTypeCode", "pyoengNames", "managementOfficeTelNo", "roadAddressPrefix", "roadZipCode"
        ]

        # 빈 컬럼을 추가하여 데이터프레임 초기화
        for key in keys + ['address', 'road_address', 'created_at', 'updated_at']:
            complex_df[key] = None

        for idx, row in complex_df.iterrows():
            _json = get_apt_complex_detail_info(base_headers, row['complexNo'])

            # 필요한 값 추출
            desired_json = {key: _json.get(key) for key in keys}

            for key, value in desired_json.items():
                complex_df.at[idx, key] = value

            # 주소 처리
            complex_df.at[idx, 'address'] = f"{desired_json.get('address', '')} {desired_json.get('detailAddress', '')}"
            complex_df.at[
                idx, 'road_address'] = f"{desired_json.get('roadAddressPrefix', '')} {desired_json.get('roadAddress', '')}"

            # 시간 추가
            complex_df.loc[idx, 'created_at'] = datetime.now()
            complex_df.loc[idx, 'updated_at'] = datetime.now()

        # 필요없는 주소 컬럼 삭제
        complex_df.drop(columns=['detailAddress', 'roadAddress', 'roadAddressPrefix'], inplace=True)

        # 컬럼 타입 지정
        complex_df['useApproveYmd'] = complex_df['useApproveYmd'].fillna(0).astype(str)
        complex_df['parkingPossibleCount'] = complex_df['parkingPossibleCount'].fillna(0).astype(int)

        # s3에 csv 파일로 저장
        upload_to_s3(bucket_name, today_complex_file_name, complex_df)

    # 5. 아파트 단지 redshift 적재
    @task
    def load_to_redshift_complex():
        today_complex_file_name = get_today_file_name(complex_file_name)
        cur = get_redshift_connection()
        cur.execute(f"""
            COPY {schema}.naver_complex
            FROM 's3://team-ariel-2-data/data/{today_complex_file_name}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1;""")
        cur.close()

    @task
    def skip_task():
        pass

    # 6. 아파트 단지마다 단지 매물 리스트 검색
    @task.short_circuit(trigger_rule='one_success')
    def process_apt_real_estate():
        # 아파트 단지번호 조회
        complex_numbers = get_complex_primary_keys(schema)
        real_estate_df_list = []

        for complexNo in complex_numbers:
            real_estate_df = get_apt_real_estate_info(base_headers, complexNo)
            real_estate_df['complexNo'] = complexNo
            real_estate_df_list.append(real_estate_df)

        real_estate_df = pd.concat(real_estate_df_list, ignore_index=True)

        # redshift 조회 후 비교하여 새로운 매물만 추출
        article_numbers = get_real_estate_primary_keys(schema)
        new_real_estate_df = real_estate_df[~real_estate_df['articleNo'].isin(article_numbers)]

        # 새로운 매물 없으면 다운스트림 테스크 생략
        if new_real_estate_df.empty:
            return False

        # 새로운 매물 있으면 다운스트림 테스크 실행
        today_real_estate_file_name = get_today_file_name(real_estate_file_name)
        upload_to_s3(bucket_name, today_real_estate_file_name, new_real_estate_df)
        return True

    # 7. 매물마다 상세정보 검색
    # 공인중개사 정보 생성, 매물 상세정보 데이터 추가
    @task
    def process_apt_real_estate_detail():
        realtor_infos = []

        # 아파트 매물 S3 조회
        today_real_estate_file_name = get_today_file_name(real_estate_file_name)
        real_estate_df = get_csv_from_s3(bucket_name, today_real_estate_file_name)

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
            if column not in real_estate_df.columns:
                real_estate_df[column] = None

        for idx, row in real_estate_df.iterrows():
            apt_detail_info = get_real_estate_detail_info(base_headers, row['articleNo'])
            articleDetail = apt_detail_info.get('articleDetail', {})
            articlePrice = apt_detail_info.get('articlePrice', {})
            articleFloor = apt_detail_info.get('articleFloor', {})
            landPrice = apt_detail_info.get('landPrice', {})
            articleRealtor = apt_detail_info.get('articleRealtor', {})
            articleOneroom = apt_detail_info.get('articleOneroom', {})
            articleFacility = apt_detail_info.get('articleFacility', {})

            # 아파트 매물 테이블에 컬럼값 추가
            real_estate_df.loc[idx, 'articleName'] = articleDetail.get('articleName')
            real_estate_df.loc[idx, 'detailAddress'] = articleDetail.get('detailAddress', '')
            real_estate_df.loc[idx, 'exposureAddress'] = articleDetail.get('exposureAddress', '')
            real_estate_df.loc[idx, 'parkingCount'] = articleDetail.get('aptParkingCount')
            real_estate_df.loc[idx, 'parkingPossibleYN'] = articleDetail.get('parkingPossibleYN')
            real_estate_df.loc[idx, 'principalUse'] = articleDetail.get('principalUse')
            real_estate_df.loc[idx, 'useApproveYmd'] = articleDetail.get('aptUseApproveYmd')
            real_estate_df.loc[idx, 'cortarNo'] = articleDetail.get('cortarNo')
            real_estate_df.loc[idx, 'roomCount'] = articleDetail.get('roomCount')
            real_estate_df.loc[idx, 'bathroomCount'] = articleDetail.get('bathroomCount')
            real_estate_df.loc[idx, 'detailDescription'] = articleDetail.get('detailDescription', '')
            real_estate_df.loc[idx, 'rentPrice'] = articlePrice.get('rentPrice')
            real_estate_df.loc[idx, 'warrantPrice'] = articlePrice.get('warrantPrice')
            real_estate_df.loc[idx, 'dealPrice'] = articlePrice.get('dealPrice')
            real_estate_df.loc[idx, 'hoNm'] = landPrice.get('hoNm')
            real_estate_df.loc[idx, 'correspondingFloorCount'] = articleFloor.get('correspondingFloorCount')
            real_estate_df.loc[idx, 'totalFloorCount'] = articleFloor.get('totalFloorCount')
            real_estate_df.loc[idx, 'etcAddress'] = articleDetail.get('etcAddress')

            if articleOneroom:
                real_estate_df.at[idx, 'roomFacilityCodes'] = articleOneroom.get('roomFacilityCodes', '')
                real_estate_df.at[idx, 'roomFacilities'] = articleOneroom.get('roomFacilities', [])
                real_estate_df.at[idx, 'buildingFacilityCodes'] = articleOneroom.get('buildingFacilityCodes', '')
                real_estate_df.at[idx, 'buildingFacilities'] = articleOneroom.get('buildingFacilities', [])
                real_estate_df.loc[idx, 'roofTopYN'] = articleOneroom.get('roofTopYN', '')
            else:
                real_estate_df.at[idx, 'roomFacilityCodes'] = articleFacility.get('lifeFacilityList', '')
                real_estate_df.at[idx, 'roomFacilities'] = articleFacility.get('lifeFacilities', [])
                real_estate_df.at[idx, 'buildingFacilityCodes'] = articleFacility.get('securityFacilityList', '')
                real_estate_df.at[idx, 'buildingFacilities'] = articleFacility.get('securityFacilities', [])

            real_estate_df.loc[idx, 'exposeStartYMD'] = articleDetail.get('exposeStartYMD')
            real_estate_df.loc[idx, 'exposeEndYMD'] = articleDetail.get('exposeEndYMD')
            real_estate_df.loc[idx, 'walkingTimeToNearSubway'] = articleDetail.get('walkingTimeToNearSubway')
            real_estate_df.loc[idx, 'heatMethodTypeCode'] = articleFacility.get('heatMethodTypeCode', '')
            real_estate_df.loc[idx, 'heatMethodTypeName'] = articleFacility.get('heatMethodTypeName', '')
            real_estate_df.loc[idx, 'heatFuelTypeCode'] = articleFacility.get('heatFuelTypeCode', '')
            real_estate_df.loc[idx, 'heatFuelTypeName'] = articleFacility.get('heatFuelTypeName', '')

            # 공인중개사 값 추가
            realtor_infos.append(articleRealtor)

        realtor_df = pd.DataFrame(realtor_infos)
        realtor_df = realtor_df.drop_duplicates(subset='realtorId')
        desired_columns = ['realtorId', 'realtorName', 'representativeName', 'address', 'establishRegistrationNo',
                           'dealCount', 'leaseCount', 'rentCount', 'latitude', 'longitude', 'representativeTelNo',
                           'cellPhoneNo', 'cortarNo']
        realtor_df = realtor_df[desired_columns]
        realtor_df['created_at'] = datetime.now()
        realtor_df['updated_at'] = datetime.now()
        realtor_df = realtor_df[realtor_df['realtorId'].notna()]

        # 아파트 매물 S3 적재
        today_real_estate_file_name = get_today_file_name(real_estate_file_name)
        upload_to_s3(bucket_name, today_real_estate_file_name, real_estate_df)

        # 공인중개사 S3 적재
        today_realtor_file_name = get_today_file_name(realtor_file_name)
        upload_to_s3(bucket_name, today_realtor_file_name, realtor_df)

    # 매물 타입 및 컬럼 전처리
    @task
    def transform_apt_real_estate():
        today_real_estate_file_name = get_today_file_name(real_estate_file_name)
        real_estate_df = get_csv_from_s3(bucket_name, today_real_estate_file_name)

        # loc를 사용하여 rentPrc 열의 NaN 값을 0으로 대체
        real_estate_df.loc[:, 'rentPrc'] = real_estate_df['rentPrc'].fillna(0)

        # 정수형 필드에 문자열이 올 경우 NaN 변환
        clean_numeric_column(real_estate_df, 'roomCount')
        clean_numeric_column(real_estate_df, 'bathroomCount')
        clean_numeric_column(real_estate_df, 'dealPrice')
        clean_numeric_column(real_estate_df, 'warrantPrice')
        clean_numeric_column(real_estate_df, 'rentPrice')

        real_estate_df['walkingTimeToNearSubway'] = real_estate_df['walkingTimeToNearSubway'].fillna(0).astype(int)
        real_estate_df['supply_area'] = real_estate_df['area1'].fillna(0).astype(int)
        real_estate_df['exclusive_area'] = real_estate_df['area2'].fillna(0).astype(int)
        real_estate_df['parkingCount'] = real_estate_df['parkingCount'].fillna(0).astype(int)
        real_estate_df['roomCount'] = real_estate_df['roomCount'].fillna(0).astype(int)
        real_estate_df['bathroomCount'] = real_estate_df['bathroomCount'].fillna(0).astype(int)
        real_estate_df['created_at'] = datetime.now()
        real_estate_df['updated_at'] = datetime.now()

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
        real_estate_df = real_estate_df[desired_columns]
        today_transform_file_name = get_today_file_name('transform_' + real_estate_file_name)
        upload_to_s3(bucket_name, today_transform_file_name, real_estate_df)

    # 새로운 매물 redshift 적재
    @task
    def load_to_redshift_real_estate():
        today_transform_file_name = get_today_file_name('transform_' + real_estate_file_name)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {schema}.naver_real_estate
                    FROM 's3://team-ariel-2-data/data/{today_transform_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {schema}.naver_real_estate')

    # 새로운 공인중개사 s3 업로드
    @task.short_circuit
    def upload_to_s3_new_realtor():
        # redshift 데이터 조회
        cur = get_redshift_connection()
        cur.execute(f'SELECT * FROM {schema}.naver_realtor;')
        redshift_df = cur.fetch_dataframe()

        # S3 데이터 조회
        today_realtor_file_name = get_today_file_name(realtor_file_name)
        s3_df = get_csv_from_s3(bucket_name, today_realtor_file_name)

        s3_df['dealCount'] = s3_df['dealCount'].fillna(0).astype(int)
        s3_df['leaseCount'] = s3_df['leaseCount'].fillna(0).astype(int)
        s3_df['rentCount'] = s3_df['rentCount'].fillna(0).astype(int)

        # redshift와 S3 비교해서 새로운 데이터만 추출
        if redshift_df.empty:
            new_df = s3_df
        else:
            new_df = s3_df[~s3_df['realtorId'].isin(redshift_df['realtorid'])]

        if new_df.empty:
            logging.info("No new realtor data")
            return False

        # 새로운 데이터 S3 적재
        today_realtor_file_name = get_today_file_name(realtor_file_name)
        upload_to_s3(bucket_name, today_realtor_file_name, new_df)
        return True

    # 새로운 공인중개사 redshift 적재
    @task
    def load_to_redshift_realtor():
        today_realtor_file_name = get_today_file_name(realtor_file_name)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {schema}.naver_realtor
                    FROM 's3://team-ariel-2-data/data/{today_realtor_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {schema}.naver_realtor')

    process_si_gun_gu_task = process_si_gun_gu()
    process_eup_myeon_dong_task = process_eup_myeon_dong()
    is_check_apt_complex = process_apt_complex()
    process_apt_complex_detail_task = process_apt_complex_detail()
    load_to_redshift_complex_task = load_to_redshift_complex()
    process_apt_real_estate = process_apt_real_estate()
    process_apt_real_estate_detail = process_apt_real_estate_detail()
    transform_apt_real_estate = transform_apt_real_estate()
    load_to_redshift_real_estate = load_to_redshift_real_estate()
    upload_to_s3_new_realtor = upload_to_s3_new_realtor()
    load_to_redshift_realtor = load_to_redshift_realtor()
    skip_task = skip_task()

    process_si_gun_gu_task >> process_eup_myeon_dong_task >> is_check_apt_complex
    is_check_apt_complex >> [process_apt_complex_detail_task, skip_task]
    process_apt_complex_detail_task >> load_to_redshift_complex_task >> process_apt_real_estate
    skip_task >> process_apt_real_estate
    process_apt_real_estate >> process_apt_real_estate_detail >> transform_apt_real_estate >> load_to_redshift_real_estate
    process_apt_real_estate_detail >> upload_to_s3_new_realtor >> load_to_redshift_realtor


naver_apt_real_estate = naver_apt_real_estate()
