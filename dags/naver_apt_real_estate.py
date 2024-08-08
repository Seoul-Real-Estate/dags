from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
import logging
from airflow.decorators import task, dag, task_group
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

SCHEMA = 'raw_data'
SEOUL_CORTARNO = 1100000000
BUCKET_NAME = 'team-ariel-2-data'
SI_GUN_GU_FILE_NAME = 'si_gun_gu.csv'
EUP_MYEON_DONG_FILE_NAME = 'eup_myeon_dong.csv'
COMPLEX_FILE_NAME = 'naver_complex.csv'
REAL_ESTATE_FILE_NAME = 'naver_real_estate.csv'
REALTOR_FILE_NAME = 'naver_realtor.csv'
REGION_URL = "https://new.land.naver.com/api/regions/list?cortarNo="
BASE_HEADERS = {
    "Accept-Encoding": "gzip",
    "Host": "new.land.naver.com",
    "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

default_args = {
    'owner': 'yong',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

iam_role = Variable.get('aws_iam_role')


def get_redshift_connection(autocommit=True):
    try:
        hook = RedshiftSQLHook(redshift_conn_id='redshift_dev')
        conn = hook.get_conn()
        conn.autocommit = autocommit
        return conn.cursor()
    except Exception as e:
        logging.error(f"Error connection to Redshift DB: {e}")
        raise


# 법정동코드(cortarNo) 지역의 동네 리스트를 데이터프레임으로 가져오기
def get_region_info(cortarNo):
    url = f"{REGION_URL}{cortarNo}"
    res = requests.get(url, headers=BASE_HEADERS)
    _json = res.json()
    return pd.DataFrame(_json["regionList"])


# 데이터프레임을 S3에 CSV파일로 업로드
def upload_to_s3(file_name, data_frame):
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
        csv_buffer = StringIO()
        key = 'data/' + file_name
        data_frame.to_csv(csv_buffer, index=False)
        s3_hook.load_string(string_data=csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        logging.info(f"Upload to S3 {BUCKET_NAME}: {file_name}")
    except Exception as e:
        logging.error(f"Error Upload to S3 {BUCKET_NAME}: {e}")
        raise


# file_name 파일이 S3 bucket_name 에 있는가?
def is_check_s3_file_exists(file_name):
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
        key = 'data/' + file_name
        return s3_hook.check_for_key(key=key, bucket_name=BUCKET_NAME)
    except Exception as e:
        logging.error(f"Error Method is [is_check_s3_file_exists]: {e}")
        raise


# 아파트 단지 리스트 가져오기
def get_apt_complex_info(cortarNo):
    complex_url = f"https://new.land.naver.com/api/regions/complexes?cortarNo={cortarNo}&realEstateType=OPST:APT&order="
    res = requests.get(complex_url, headers=BASE_HEADERS)
    return pd.DataFrame(res.json()["complexList"])


# 아파트 단지 상세정보 가져오기
def get_apt_complex_detail_info(complexNo):
    url = f"https://new.land.naver.com/api/complexes/{complexNo}"
    params = {"sameAddressGroup": "false"}
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get('naver_token')
    headers.update({"Authorization": f"{naver_token}"})

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
def get_apt_real_estate_info(complexNo):
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
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get('naver_token')
    headers.update({"Authorization": f"{naver_token}"})
    res = requests.get(url, params=params, headers=headers)
    _json = res.json()
    return pd.DataFrame(_json["articleList"])


# 아파트 단지 기본키(complexNo) 가져오기
def get_complex_primary_keys():
    cur = get_redshift_connection()
    cur.execute(f'SELECT complexNo FROM {SCHEMA}.naver_complex')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


# 매물 기본키(articleNo) 가져오기
def get_real_estate_primary_keys():
    cur = get_redshift_connection()
    cur.execute(f'SELECT articleNo FROM {SCHEMA}.naver_real_estate')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


# 아파트 매물 상세정보 가져오기 (공인중개사 정보 포함)
def get_real_estate_detail_info(articleNo):
    url = f"https://new.land.naver.com/api/articles/{articleNo}"
    params = {"complexNo": ""}
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get('naver_token')
    headers.update({"Authorization": f"{naver_token}"})
    return requests.get(url, params=params, headers=headers).json()


# 숫자 변환 시 에러가 발생하는 값은 NaN으로 대체
def clean_numeric_column(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')


# s3의 data/ 폴더의 file_name CSV 파일 가져오기
def get_csv_from_s3(file_name):
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
        key = 'data/' + file_name
        return pd.read_csv(s3_hook.get_key(key=key, bucket_name=BUCKET_NAME).get()['Body'])
    except Exception as e:
        logging.error(f"Error Method is [get_csv_from_s3]: {e}")
        raise


def get_today_file_name(file_name):
    today = datetime.now().strftime('%Y-%m-%d')
    return f'{today}_{file_name}'


def add_necessary_columns(df, columns):
    for column in columns:
        if column not in df.columns:
            df[column] = None


def add_apt_detail_columns(idx, apt_df, apt_detail_info):
    try:
        articleDetail = apt_detail_info.get('articleDetail', {})
        articlePrice = apt_detail_info.get('articlePrice', {})
        articleFloor = apt_detail_info.get('articleFloor', {})
        landPrice = apt_detail_info.get('landPrice', {})
        articleOneroom = apt_detail_info.get('articleOneroom', {})
        articleFacility = apt_detail_info.get('articleFacility', {})

        # 아파트 매물 테이블에 컬럼값 추가
        apt_df.loc[idx, 'articleName'] = articleDetail.get('articleName')
        apt_df.loc[idx, 'detailAddress'] = articleDetail.get('detailAddress', '')
        apt_df.loc[idx, 'exposureAddress'] = articleDetail.get('exposureAddress', '')
        apt_df.loc[idx, 'parkingCount'] = articleDetail.get('aptParkingCount')
        apt_df.loc[idx, 'parkingPossibleYN'] = articleDetail.get('parkingPossibleYN')
        apt_df.loc[idx, 'principalUse'] = articleDetail.get('principalUse')
        apt_df.loc[idx, 'useApproveYmd'] = articleDetail.get('aptUseApproveYmd')
        apt_df.loc[idx, 'cortarNo'] = articleDetail.get('cortarNo')
        apt_df.loc[idx, 'roomCount'] = articleDetail.get('roomCount')
        apt_df.loc[idx, 'bathroomCount'] = articleDetail.get('bathroomCount')
        apt_df.loc[idx, 'detailDescription'] = articleDetail.get('detailDescription', '')
        apt_df.loc[idx, 'rentPrice'] = articlePrice.get('rentPrice')
        apt_df.loc[idx, 'warrantPrice'] = articlePrice.get('warrantPrice')
        apt_df.loc[idx, 'dealPrice'] = articlePrice.get('dealPrice')
        apt_df.loc[idx, 'hoNm'] = landPrice.get('hoNm')
        apt_df.loc[idx, 'correspondingFloorCount'] = articleFloor.get('correspondingFloorCount')
        apt_df.loc[idx, 'totalFloorCount'] = articleFloor.get('totalFloorCount')
        apt_df.loc[idx, 'etcAddress'] = articleDetail.get('etcAddress')

        if articleOneroom:
            apt_df.at[idx, 'roomFacilityCodes'] = articleOneroom.get('roomFacilityCodes', '')
            apt_df.at[idx, 'roomFacilities'] = articleOneroom.get('roomFacilities', [])
            apt_df.at[idx, 'buildingFacilityCodes'] = articleOneroom.get('buildingFacilityCodes', '')
            apt_df.at[idx, 'buildingFacilities'] = articleOneroom.get('buildingFacilities', [])
            apt_df.loc[idx, 'roofTopYN'] = articleOneroom.get('roofTopYN', '')
        else:
            apt_df.at[idx, 'roomFacilityCodes'] = articleFacility.get('lifeFacilityList', '')
            apt_df.at[idx, 'roomFacilities'] = articleFacility.get('lifeFacilities', [])
            apt_df.at[idx, 'buildingFacilityCodes'] = articleFacility.get('securityFacilityList', '')
            apt_df.at[idx, 'buildingFacilities'] = articleFacility.get('securityFacilities', [])

        apt_df.loc[idx, 'exposeStartYMD'] = articleDetail.get('exposeStartYMD')
        apt_df.loc[idx, 'exposeEndYMD'] = articleDetail.get('exposeEndYMD')
        apt_df.loc[idx, 'walkingTimeToNearSubway'] = articleDetail.get('walkingTimeToNearSubway')
        apt_df.loc[idx, 'heatMethodTypeCode'] = articleFacility.get('heatMethodTypeCode', '')
        apt_df.loc[idx, 'heatMethodTypeName'] = articleFacility.get('heatMethodTypeName', '')
        apt_df.loc[idx, 'heatFuelTypeCode'] = articleFacility.get('heatFuelTypeCode', '')
        apt_df.loc[idx, 'heatFuelTypeName'] = articleFacility.get('heatFuelTypeName', '')
    except Exception as e:
        logging.error(f"Error Method is [add_apt_detail_columns]: {e}")
        raise


def get_naver_realtor_pk():
    cur = get_redshift_connection()
    cur.execute(f'SELECT realtorid FROM {SCHEMA}.naver_realtor')
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


def transform_apt_df(real_estate_df):
    clean_numeric_column(real_estate_df, 'roomCount')
    clean_numeric_column(real_estate_df, 'bathroomCount')
    clean_numeric_column(real_estate_df, 'dealPrice')
    clean_numeric_column(real_estate_df, 'warrantPrice')
    clean_numeric_column(real_estate_df, 'rentPrice')
    real_estate_df.loc[:, 'rentPrc'] = real_estate_df['rentPrc'].fillna(0)
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
    return real_estate_df


@dag(
    default_args=default_args,
    description="네이버부동산 아파트/오피스텔 데이터 수집 및 적재 DAG",
    schedule_interval='0 12 * * *',
    start_date=datetime(2024, 8, 4),
    catchup=False,
    tags=['daily', 'real_estate', 'naver', 'apt']
)
def naver_apt_real_estate():
    @task_group(
        group_id="fetch_and_process_region_data",
        tooltip="서울 전지역 읍면동 정보 저장"
    )
    def fetch_and_process_region_data():
        # 1. 시/군/구 정보 검색 API
        @task
        def fetch_si_gun_gu():
            if is_check_s3_file_exists(SI_GUN_GU_FILE_NAME):
                return

            df = get_region_info(SEOUL_CORTARNO)
            upload_to_s3(SI_GUN_GU_FILE_NAME, df)

        # 2. 읍/면/동 정보 검색 API
        @task
        def fetch_eup_myeon_dong():
            if is_check_s3_file_exists(EUP_MYEON_DONG_FILE_NAME):
                return

            eup_myeon_dong_df_list = []
            si_gun_gu_df = get_csv_from_s3(SI_GUN_GU_FILE_NAME)
            for cortarNo in si_gun_gu_df['cortarNo']:
                eup_myeon_dong_df = get_region_info(cortarNo)
                eup_myeon_dong_df_list.append(eup_myeon_dong_df)

            eup_myeon_dong_df = pd.concat(eup_myeon_dong_df_list, ignore_index=True)
            upload_to_s3(EUP_MYEON_DONG_FILE_NAME, eup_myeon_dong_df)

        fetch_si_gun_gu() >> fetch_eup_myeon_dong()

    @task_group(
        group_id="fetch_and_process_apt_complex_data",
        tooltip="아파트/오피스텔 새로운 단지 정보 CSV 저장"
    )
    def fetch_and_process_apt_complex_data():
        # 3. 동마다 아파트 단지 리스트 검색
        @task.branch
        def process_apt_complex():
            dong_df = get_csv_from_s3(EUP_MYEON_DONG_FILE_NAME)
            complex_df_list = []

            for cortarNo in dong_df['cortarNo']:
                complex_df = get_apt_complex_info(cortarNo)
                complex_df_list.append(complex_df)

            complex_df = pd.concat(complex_df_list, ignore_index=True)
            complex_df.drop(
                columns=['detailAddress', 'totalBuildingCount', 'highFloor', 'lowFloor', 'cortarAddress', 'dealCount',
                         'leaseCount', 'rentCount', 'shortTermRentCount'], inplace=True)

            complex_numbers = get_complex_primary_keys()
            new_complex_df = complex_df[~complex_df['complexNo'].isin(complex_numbers)]

            if new_complex_df.empty:
                return 'fetch_and_process_apt_complex_data.skip_task'

            today_complex_file_name = get_today_file_name(COMPLEX_FILE_NAME)
            upload_to_s3(today_complex_file_name, new_complex_df)
            return 'fetch_and_process_apt_complex_data.process_apt_complex_detail'

        # 4. 단지마다 아파트 단지 상세정보 검색
        @task
        def process_apt_complex_detail():
            today_complex_file_name = get_today_file_name(COMPLEX_FILE_NAME)
            complex_df = get_csv_from_s3(today_complex_file_name)

            # 상세 정보에서 필요한 데이터 key
            keys = [
                "address", "detailAddress", "roadAddress", "roadAddressPrefix", "maxSupplyArea", "minSupplyArea",
                "parkingPossibleCount", "parkingCountByHousehold", "constructionCompanyName", "heatMethodTypeCode",
                "heatFuelTypeCode", "pyoengNames", "managementOfficeTelNo", "roadAddressPrefix", "roadZipCode"
            ]

            add_necessary_columns(complex_df, keys + ['address', 'road_address', 'created_at', 'updated_at'])
            for idx, row in complex_df.iterrows():
                _json = get_apt_complex_detail_info(row['complexNo'])
                desired_json = {key: _json.get(key) for key in keys}

                for key, value in desired_json.items():
                    complex_df.at[idx, key] = value

                address = f"{desired_json.get('address', '')} {desired_json.get('detailAddress', '')}"
                road_address = f"{desired_json.get('roadAddressPrefix', '')} {desired_json.get('roadAddress', '')}"
                complex_df.at[idx, 'address'] = address
                complex_df.at[idx, 'road_address'] = road_address
                complex_df.loc[idx, 'created_at'] = datetime.now()
                complex_df.loc[idx, 'updated_at'] = datetime.now()

            complex_df.drop(columns=['detailAddress', 'roadAddress', 'roadAddressPrefix'], inplace=True)
            complex_df['useApproveYmd'] = complex_df['useApproveYmd'].fillna(0).astype(str)
            complex_df['parkingPossibleCount'] = complex_df['parkingPossibleCount'].fillna(0).astype(int)
            upload_to_s3(today_complex_file_name, complex_df)

        # 5. 아파트 단지 redshift 적재
        @task
        def load_to_redshift_complex():
            today_complex_file_name = get_today_file_name(COMPLEX_FILE_NAME)
            cur = get_redshift_connection()
            cur.execute(f"""
                COPY {SCHEMA}.naver_complex
                FROM 's3://team-ariel-2-data/data/{today_complex_file_name}'
                IAM_ROLE '{iam_role}'
                CSV
                IGNOREHEADER 1;""")
            cur.close()

        @task
        def skip_task():
            pass

        process_apt_complex_detail = process_apt_complex_detail()
        redshift_complex = load_to_redshift_complex()
        skip_task = skip_task()
        process_apt_complex() >> [process_apt_complex_detail, skip_task]
        process_apt_complex_detail >> redshift_complex

    @task_group(
        group_id="fetch_and_process_apt",
        tooltip="아파트/오피스텔 새로운 매물 정보 CSV 저장"
    )
    def fetch_and_process_apt_real_estate():
        # 6. 아파트 단지마다 새로운 아파트 매물 리스트 검색
        @task.short_circuit(trigger_rule='one_success')
        def process_apt_real_estate():
            complex_numbers = get_complex_primary_keys()
            real_estate_df_list = []

            for complexNo in complex_numbers:
                real_estate_df = get_apt_real_estate_info(complexNo)
                real_estate_df['complexNo'] = complexNo
                real_estate_df_list.append(real_estate_df)

            real_estate_df = pd.concat(real_estate_df_list, ignore_index=True)
            article_numbers = get_real_estate_primary_keys()
            new_real_estate_df = real_estate_df[~real_estate_df['articleNo'].isin(article_numbers)]
            if new_real_estate_df.empty:
                return False

            today_real_estate_file_name = get_today_file_name(REAL_ESTATE_FILE_NAME)
            upload_to_s3(today_real_estate_file_name, new_real_estate_df)
            return True

        # 7. 매물마다 상세정보 검색 및 공인중개사 정보 생성
        @task
        def process_apt_real_estate_detail():
            today_real_estate_file_name = get_today_file_name(REAL_ESTATE_FILE_NAME)
            apt_df = get_csv_from_s3(today_real_estate_file_name)
            realtor_infos = []
            necessary_columns = [
                'complexNo', 'articleName', 'detailAddress', 'exposureAddress', 'parkingCount', 'parkingPossibleYN',
                'principalUse', 'useApproveYmd', 'cortarNo', 'roomCount', 'bathroomCount', 'detailDescription',
                'rentPrice', 'warrantPrice', 'dealPrice', 'hoNm', 'correspondingFloorCount', 'totalFloorCount',
                'address', 'roadAddress', 'etcAddress', 'roomFacilityCodes', 'roomFacilities', 'buildingFacilityCodes',
                'buildingFacilities', 'roofTopYN', 'exposeStartYMD', 'exposeEndYMD', 'walkingTimeToNearSubway',
                'heatMethodTypeCode', 'heatMethodTypeName', 'heatFuelTypeCode', 'heatFuelTypeName', 'supply_area',
                'exclusive_area'
            ]
            add_necessary_columns(apt_df, necessary_columns)
            for idx, row in apt_df.iterrows():
                apt_detail_info = get_real_estate_detail_info(row['articleNo'])
                add_apt_detail_columns(idx, apt_df, apt_detail_info)
                realtor = apt_detail_info.get('articleRealtor', {})
                realtor_infos.append(realtor)

            today_real_estate_file_name = get_today_file_name(REAL_ESTATE_FILE_NAME)
            upload_to_s3(today_real_estate_file_name, apt_df)
            realtor_df = pd.DataFrame(realtor_infos)
            realtor_df = realtor_df.drop_duplicates(subset='realtorId')
            realtor_df = realtor_df[realtor_df['realtorId'].notna()]
            today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
            upload_to_s3(today_realtor_file_name, realtor_df)

        process_apt_real_estate() >> process_apt_real_estate_detail()

    @task
    def transform_apt_real_estate():
        today_real_estate_file_name = get_today_file_name(REAL_ESTATE_FILE_NAME)
        apt_df = get_csv_from_s3(today_real_estate_file_name)
        real_estate_df = transform_apt_df(apt_df)
        today_transform_file_name = get_today_file_name('transform_' + REAL_ESTATE_FILE_NAME)
        upload_to_s3(today_transform_file_name, real_estate_df)

    @task
    def load_to_redshift_real_estate():
        today_transform_file_name = get_today_file_name('transform_' + REAL_ESTATE_FILE_NAME)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {SCHEMA}.naver_real_estate
                    FROM 's3://team-ariel-2-data/data/{today_transform_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {SCHEMA}.naver_real_estate')

    @task.short_circuit
    def extract_new_realtor():
        today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
        realtor_df = get_csv_from_s3(today_realtor_file_name)
        realtor_ids = get_naver_realtor_pk()
        new_realtor_df = realtor_df[~realtor_df['realtorId'].isin(realtor_ids)]
        if new_realtor_df.empty:
            logging.info("No new realtor data")
            return False

        upload_to_s3(today_realtor_file_name, new_realtor_df)
        return True

    @task
    def transform_realtor():
        today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
        realtor_df = get_csv_from_s3(today_realtor_file_name)
        desired_columns = ['realtorId', 'realtorName', 'representativeName', 'address', 'establishRegistrationNo',
                           'dealCount', 'leaseCount', 'rentCount', 'latitude', 'longitude', 'representativeTelNo',
                           'cellPhoneNo', 'cortarNo']
        realtor_df = realtor_df[desired_columns]
        realtor_df['dealCount'] = realtor_df['dealCount'].fillna(0).astype(int)
        realtor_df['leaseCount'] = realtor_df['leaseCount'].fillna(0).astype(int)
        realtor_df['rentCount'] = realtor_df['rentCount'].fillna(0).astype(int)
        realtor_df['created_at'] = datetime.now()
        realtor_df['updated_at'] = datetime.now()
        today_transform_realtor_file_name = get_today_file_name('transform_' + REALTOR_FILE_NAME)
        upload_to_s3(today_transform_realtor_file_name, realtor_df)

    @task
    def load_to_redshift_realtor():
        today_transform_realtor_file_name = get_today_file_name('transform_' + REALTOR_FILE_NAME)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {SCHEMA}.naver_realtor
                    FROM 's3://team-ariel-2-data/data/{today_transform_realtor_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {SCHEMA}.naver_realtor')

    fetch_region = fetch_and_process_region_data()
    fetch_apt_complex = fetch_and_process_apt_complex_data()
    fetch_apt_real_estate = fetch_and_process_apt_real_estate()
    transform_apt_real_estate = transform_apt_real_estate()
    load_to_redshift_real_estate = load_to_redshift_real_estate()
    transform_realtor = transform_realtor()
    extract_new_realtor = extract_new_realtor()
    load_to_redshift_realtor = load_to_redshift_realtor()

    fetch_region >> fetch_apt_complex >> fetch_apt_real_estate
    fetch_apt_real_estate >> transform_apt_real_estate >> load_to_redshift_real_estate
    fetch_apt_real_estate >> extract_new_realtor >> transform_realtor >> load_to_redshift_realtor


naver_apt_real_estate = naver_apt_real_estate()
