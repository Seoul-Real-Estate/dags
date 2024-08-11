import ast
import time
from datetime import datetime, timedelta, date
from io import StringIO

import pandas as pd
import requests
import logging
import json
from airflow.decorators import task, dag, task_group
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from requests.adapters import HTTPAdapter
from urllib3 import Retry

SCHEMA = "raw_data"
BUCKET_NAME = "team-ariel-2-data"
SEOUL_CORTARNO = 1100000000
SI_GUN_GU_FILE_NAME = "si_gun_gu.csv"
EUP_MYEON_DONG_FILE_NAME = "eup_myeon_dong.csv"
DABANG_APT_FILE_NAME = "dabang_apt_real_estate.csv"
DABANG_REALTOR_FILE_NAME = "dabang_realtor.csv"
DABANG_APT_COMPLEX_FILE_NAME = "dabang_complex.csv"
REGION_URL = "https://new.land.naver.com/api/regions/list?cortarNo="
NAVER_SEARCH_URL = "https://map.naver.com/p/api/search/allSearch"
NAVER_COORDINATE_URL = "https://map.naver.com/p/api/polygon"
DABANG_APT_URL = "https://www.dabangapp.com/api/v5/room-list/category/apt/bbox"
DABANG_APT_DETAIL_URL = "https://www.dabangapp.com/api/3/new-room/detail"
DABANG_COMPLEX_DETAIL_URL = "https://www.dabangapp.com/api/3/new-complex/"
BASE_HEADERS = {
    "Accept-Encoding": "gzip",
    "Host": "new.land.naver.com",
    "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}
DABANG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"}
DABANG_APT_HEADERS = {
    "D-Api-Version": "5.0.0",
    "D-App-Version": "1",
    "D-Call-Type": "web",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}
DABANG_APT_FILTERS = {
    "sellingTypeList": ["SELL", "MONTHLY_RENT", "LEASE"],
    "tradeRange": {"min": 0, "max": 999999},
    "depositRange": {"min": 0, "max": 999999},
    "priceRange": {"min": 0, "max": 999999},
    "isIncludeMaintenance": False,
    "pyeongRange": {"min": 0, "max": 999999},
    "useApprovalDateRange": {"min": 0, "max": 999999},
    "dealTypeList": ["AGENT", "DIRECT"],
    "householdNumRange": {"min": 0, "max": 999999},
    "parkingNumRange": {"min": 0, "max": 999999},
    "isShortLease": False,
    "hasTakeTenant": False
}

default_args = {
    "owner": "yong",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

iam_role = Variable.get("aws_iam_role")


def get_redshift_connection(autocommit=True):
    try:
        hook = RedshiftSQLHook(redshift_conn_id="redshift_dev")
        conn = hook.get_conn()
        conn.autocommit = autocommit
        return conn.cursor()
    except Exception as e:
        logging.error(f"Error connection to Redshift DB: {e}")
        raise


def is_check_s3_file_exists(file_name):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        key = "data/" + file_name
        return s3_hook.check_for_key(key=key, bucket_name=BUCKET_NAME)
    except Exception as e:
        logging.error(f"Error Method is [is_check_s3_file_exists]: {e}")
        raise


def upload_to_s3(file_name, data_frame):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        csv_buffer = StringIO()
        key = "data/" + file_name
        data_frame.to_csv(csv_buffer, index=False)
        s3_hook.load_string(string_data=csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        logging.info(f"Upload to S3 {BUCKET_NAME}: {file_name}")
    except Exception as e:
        logging.error(f"Error Upload to S3 {BUCKET_NAME}: {e}")
        raise


def get_csv_from_s3(file_name, dtype_spec=None):
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
        key = 'data/' + file_name
        csv_data = s3_hook.get_key(key=key, bucket_name=BUCKET_NAME).get()['Body']
        return pd.read_csv(csv_data, dtype=dtype_spec) if dtype_spec else pd.read_csv(csv_data)
    except FileNotFoundError as fe:
        logging.error(
            f"FileNotFoundError: The file '{file_name}' could not be found in the S3 bucket '{BUCKET_NAME}'. "
            f"Check the S3 permissions as well. Detailed error: {fe}"
        )
        raise
    except Exception as e:
        logging.error(
            f"Unexpected error: An unexpected error occurred while attempting to read the CSV file from S3 at path '{key}'. "
            f"Detailed error: {e}"
        )
        raise


def get_today_file_name(file_name):
    today = datetime.now().strftime("%Y-%m-%d")
    return f"{today}_{file_name}"


def get_region_info(cortar_no):
    url = f"{REGION_URL}{cortar_no}"
    res = requests.get(url, headers=BASE_HEADERS)
    _json = res.json()
    return pd.DataFrame(_json["regionList"])


def requests_retry_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=30, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def get_naver_search(keyword):
    params = {
        "query": keyword,
        "type": all,
        "searchCoord": f"{0};{0}",
        "boundary": ""
    }

    try:
        session = requests_retry_session()
        res = session.get(NAVER_SEARCH_URL, params=params, headers=DABANG_HEADERS)
        res.raise_for_status()
        return res.json()["result"]
    except Exception as e:
        logging.error(f"Request failed: {e}; keyword: '{keyword}'")
        raise


def get_naver_coordinate(x, y, keyword_rcode):
    params = {
        "lat": x,
        "lng": y,
        "order": "adm",
        "keyword": keyword_rcode,
        "zoom": 16,
    }

    try:
        session = requests_retry_session()
        res = session.get(NAVER_COORDINATE_URL, params=params, headers=DABANG_HEADERS)
        res.raise_for_status()
        return res.json().json()["features"][0]["bbox"]
    except Exception as e:
        logging.error(f"Request failed: {e}; keyword_rcode: '{keyword_rcode}'")
        raise


def get_dabang_apt(sw, ne):
    bbox = {
        "sw": sw,
        "ne": ne
    }

    room_df_list = []
    page = 1
    while True:
        params = {
            "bbox": json.dumps(bbox),
            "filters": json.dumps(DABANG_APT_FILTERS),
            "page": page,
            "useMap": "naver",
            "zoom": "14",
        }

        res = requests.get(DABANG_APT_URL, params=params, headers=DABANG_APT_HEADERS)
        apt_json = res.json()
        room_df = pd.DataFrame(apt_json["result"]["roomList"])
        room_df_list.append(room_df)

        if not apt_json["result"]["hasMore"]:
            break

        page += 1

    return pd.concat(room_df_list, ignore_index=True)


def get_apt_detail_info(room_id):
    params = {
        "api_version": "3.0.1",
        "call_type": "web",
        "room_id": room_id,
        "version": "1"
    }

    session = requests_retry_session()
    try:
        res = session.get(DABANG_APT_DETAIL_URL, params=params, headers=DABANG_HEADERS)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.HTTPError as he:
        if 400 <= he.response.status_code < 500:
            logging.warning(f"Warning '{he.response}' message: '{res.text}' error: '{he}'")
            return None

        logging.warning(f"Error '{he.response}' message: '{res.text}' error: '{he}'")
        raise
    except Exception as e:
        logging.warning(f"Error [get_apt_detail_info] message:'{e}'")
        raise


def get_complex_detail_info(complex_id):
    url = DABANG_COMPLEX_DETAIL_URL + complex_id
    params = {
        "api_version": "3.0.1",
        "call_type": "web",
        "complex_id": complex_id,
        "version": "1"
    }
    try:
        session = requests_retry_session()
        res = session.get(url, params=params, headers=DABANG_HEADERS)
        res.raise_for_status()
        return res.json()["complex"]
    except Exception as e:
        logging.error(f"Request [get_complex_detail_info] failed: {e}; complex_id: '{complex_id}'")
        raise


def get_all_dabang_real_estate_pk():
    cur = get_redshift_connection()
    cur.execute(f"SELECT id FROM {SCHEMA}.dabang_real_estate")
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


def get_all_dabang_complex_pk():
    cur = get_redshift_connection()
    cur.execute(f"SELECT complex_id FROM {SCHEMA}.dabang_complex")
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


def get_all_dabang_realtor_pk():
    cur = get_redshift_connection()
    cur.execute(f"SELECT id FROM {SCHEMA}.dabang_realtor")
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


def add_necessary_columns(df, columns):
    for column in columns:
        if column not in df.columns:
            df[column] = None


def transform_complex_address(complex):
    if complex["jibun_address"] is not None:
        complex["jibun_address"] = complex["jibun_address"].split(',')[0]

    if complex["road_address"] is not None:
        complex['road_address'] = complex['road_address'].split(',')[0]

    return complex


def add_apt_detail_data(apt_df, idx, room):
    try:
        apt_df.loc[idx, 'dabang_realtor_id'] = room.get("realtor_id")
        apt_df.loc[idx, 'dabang_complex_id'] = room.get('complex_id')
        apt_df.loc[idx, 'address'] = room.get('full_jibun_address_str')
        apt_df.loc[idx, 'road_address'] = room.get('full_road_address_str')
        apt_df.loc[idx, 'building_use_types_str'] = room.get('building_use_types_str')[0] if room.get(
            'building_use_types_str') else None
        apt_df.loc[idx, 'contact_number'] = room.get('call_number')
        apt_df.loc[idx, 'memo'] = room.get('memo')
        apt_df.loc[idx, 'rent_price'] = room.get('price_info')[0][1]
        apt_df.loc[idx, 'dong'] = room.get('dong')
        apt_df.loc[idx, 'ho'] = room.get('ho')
        apt_df.loc[idx, 'saved_time_str'] = room.get('saved_time_str')  # 최초등록일
        apt_df.loc[idx, 'roomTypeName'] = room.get('room_type_str')
        apt_df.loc[idx, 'heating'] = room.get('heating')
        apt_df.loc[idx, 'room_floor_str'] = room.get('room_floor_str')
        apt_df.loc[idx, 'building_floor_str'] = room.get('building_floor_str')
        apt_df.loc[idx, 'building_approval_date_str'] = room.get('building_approval_date_str')
        apt_df.loc[idx, 'bath_num'] = room.get('bath_num')
        apt_df.loc[idx, 'beds_num'] = room.get('beds_num')
        apt_df.loc[idx, 'maintenance_cost'] = room.get('maintenance_cost')
        apt_df.at[idx, 'hash_tags'] = room.get('hash_tags', '')
        apt_df.loc[idx, 'parking'] = room.get('parking')
        apt_df.loc[idx, 'parking_num'] = room.get('parking_num')
        apt_df.loc[idx, 'elevator_str'] = room.get('elevator_str')
        apt_df.loc[idx, 'loan_str'] = room.get('loan_str')
        apt_df.loc[idx, 'direction_str'] = room.get('direction_str')
        apt_df.loc[idx, 'room_size'] = room.get('room_size')  # 전용면적
        apt_df.loc[idx, 'provision_size'] = room.get('provision_size')  # 공급면적
        price_info = room.get('price_info')
        if price_info[0][2] == 2:  # 매매
            apt_df.loc[idx, 'deal_price'] = price_info[0][0]
        else:
            apt_df.loc[idx, 'warrant_price'] = price_info[0][0]

        if room.get('room_options') is not None:
            room_option_names = [item['name'] for item in room.get('room_options')]
            apt_df.at[idx, 'room_options'] = room_option_names

        if room.get('safeties') is not None:
            safeties_names = [item['name'] for item in room.get('safeties')]
            apt_df.at[idx, 'safeties'] = safeties_names

    except Exception as e:
        logging.error(f"Failed to process room data for index {idx} with error: {e}; room data: {room}")
        raise


def fill_missing_dabang_numeric_values(df, columns):
    df[columns] = df[columns].fillna(0)
    return df


@dag(
    default_args=default_args,
    description="다방 아파트 단지 및 매물, 공인중개사 데이터 수집 및 적재 DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    tags=["daily", "real_estate", "dabang", "apt"]
)
def dabang_apt_real_estate():
    @task_group(
        group_id="fetch_and_process_coordinate_region_data",
        tooltip="서울 전지역 읍/면/동 S3 업로드"
    )
    def fetch_and_process_region_data():
        @task
        def fetch_si_gun_gu():
            if is_check_s3_file_exists(SI_GUN_GU_FILE_NAME):
                return

            df = get_region_info(SEOUL_CORTARNO)
            upload_to_s3(SI_GUN_GU_FILE_NAME, df)

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

        @task
        def add_coordinate_to_eup_myeon_dong():
            dong_df = get_csv_from_s3(EUP_MYEON_DONG_FILE_NAME)
            if "sw_lat" in dong_df.columns and "sw_lng" in dong_df.columns and "ne_lat" in dong_df.columns and "ne_lng" in dong_df.columns:
                if dong_df[["sw_lat", "sw_lng", "ne_lat", "ne_lng"]].notnull().all().all():
                    return

            for idx, row in dong_df.iterrows():
                res = get_naver_search(row["cortarName"])
                rcode = res["place"]["list"][0]["rcode"]
                lat = float(res["place"]["list"][0]["x"])
                lng = float(res["place"]["list"][0]["y"])
                bbox = get_naver_coordinate(lat, lng, rcode)
                dong_df.loc[idx, "sw_lat"] = bbox[1]
                dong_df.loc[idx, "sw_lng"] = bbox[0]
                dong_df.loc[idx, "ne_lat"] = bbox[3]
                dong_df.loc[idx, "ne_lng"] = bbox[2]
                time.sleep(1)

            upload_to_s3(EUP_MYEON_DONG_FILE_NAME, dong_df)

        fetch_si_gun_gu() >> fetch_eup_myeon_dong() >> add_coordinate_to_eup_myeon_dong()

    @task_group(
        group_id="fetch_and_process_apt_data",
        tooltip="아파트 매물, 단지, 공인중개사 정보 S3 업로드"
    )
    def fetch_and_process_apt_complex_realtor_data():
        @task
        def fetch_apt():
            apt_df_list = []
            dong_df = get_csv_from_s3(EUP_MYEON_DONG_FILE_NAME)
            for idx, row in dong_df.iterrows():
                sw = {"lat": row["sw_lat"], "lng": row["sw_lng"]}
                ne = {"lat": row["ne_lat"], "lng": row["ne_lng"]}
                apt_df = get_dabang_apt(sw, ne)
                apt_df_list.append(apt_df)

            apt_df = pd.concat(apt_df_list, ignore_index=True)
            desired_columns = ["seq", "id", "roomTypeName", "randomLocation", "complexName", "roomTitle", "roomDesc",
                               "priceTypeName", "priceTitle"]
            apt_df = apt_df[desired_columns]
            today_apt_file_name = get_today_file_name(DABANG_APT_FILE_NAME)
            upload_to_s3(today_apt_file_name, apt_df)

        @task.short_circuit
        def extract_new_apt():
            today_apt_file_name = get_today_file_name(DABANG_APT_FILE_NAME)
            apt_df = get_csv_from_s3(today_apt_file_name)
            real_estate_pk_list = get_all_dabang_real_estate_pk()
            new_apt_df = apt_df[~apt_df["id"].isin(real_estate_pk_list)]
            if new_apt_df.empty:
                return False

            upload_to_s3(today_apt_file_name, new_apt_df)
            return True

        @task
        def transform_apt():
            today_apt_file_name = get_today_file_name(DABANG_APT_FILE_NAME)
            apt_df = get_csv_from_s3(today_apt_file_name)
            if "randomLocation" in apt_df.columns:
                apt_df['randomLocation'] = apt_df['randomLocation'].apply(ast.literal_eval)
                apt_df.loc[:, "latitude"] = apt_df["randomLocation"].apply(lambda x: x["lat"])
                apt_df.loc[:, "longitude"] = apt_df["randomLocation"].apply(lambda x: x["lng"])
                apt_df.drop(columns=['randomLocation'], inplace=True)

            necessary_columns = ["address", "road_address", "bath_num", "beds_num", "building_approval_date_str",
                                 "building_floor_str", "building_use_types_str", "complex_id", "dong", "ho",
                                 "direction_str", "hash_tags", "maintenance_cost", "memo", "parking", "parking_num",
                                 "elevator_str", "loan_str", "deal_price", "warrant_price", "rent_price",
                                 "provision_size", "room_size", "saved_time_str", "room_options", "safeties",
                                 "contact_number", "realtor_id", "heating", "room_floor_str"
                                 ]
            add_necessary_columns(apt_df, necessary_columns)

            numeric_columns = [
                'latitude', 'longitude', 'maintenance_cost', 'deal_price',
                'warrant_price', 'rent_price', 'room_size', 'provision_size',
                'bath_num', 'beds_num', 'parking_num'
            ]
            fill_missing_dabang_numeric_values(apt_df, numeric_columns)
            apt_df["parking_num"] = apt_df["parking_num"].fillna(0).astype(int)
            upload_to_s3(today_apt_file_name, apt_df)

        @task
        def fetch_apt_detail_and_complex_and_realtor():
            realtor_infos = []
            apt_complex_infos = []
            today_apt_file_name = get_today_file_name(DABANG_APT_FILE_NAME)
            dtype_spec = {
                "seq": str,
                "id": str,
                "roomTypeName": str,
                "complexName": str,
                "roomTitle": str,
                "roomDesc": str,
                "priceTypeName": str,
                "priceTitle": str,
                "latitude": float,
                "longitude": float,
                "address": str,
                "road_address": str,
                "bath_num": int,
                "beds_num": int,
                "building_approval_date_str": str,
                "building_floor_str": str,
                "building_use_types_str": str,
                "complex_id": str,
                "dong": str,
                "ho": str,
                "direction_str": str,
                "hash_tags": str,
                "maintenance_cost": int,
                "memo": str,
                "parking": str,
                "parking_num": int,
                "elevator_str": str,
                "loan_str": str,
                "deal_price": int,
                "warrant_price": int,
                "rent_price": int,
                "provision_size": float,
                "room_size": float,
                "saved_time_str": str,
                "room_options": str,
                "safeties": str,
                "contact_number": str,
                "realtor_id": str,
                "heating": str,
                "room_floor_str": str
            }
            apt_df = get_csv_from_s3(today_apt_file_name, dtype_spec)
            for idx, row in apt_df.iterrows():
                _json = get_apt_detail_info(row["id"])
                if _json is None:
                    continue
                room = _json["room"]
                complex = _json["complex"]
                realtor = _json["agent"]
                contact = _json["contact"]

                apt_complex_infos.append(transform_complex_address(complex))
                # 공인중개사 정보가 없는 경우는 직거래
                realtor_id = ""
                if realtor is not None:
                    logging.info(f"room_id: {row["id"]}")
                    logging.info(realtor)
                    if realtor.get("location") is not None:
                        realtor["longitude"] = realtor["location"][0]
                        realtor["latitude"] = realtor["location"][1]
                    else:
                        realtor["longitude"] = 0.0
                        realtor["latitude"] = 0.0
                    logging.info(
                        f"{row['id']} | realtor longitude: '{realtor['longitude']}' latitude: '{realtor['latitude']}'")
                    realtor_infos.append(realtor)
                    realtor_id = realtor.get("id", "")
                else:
                    logging.info(f"{row['id']} | realtor information is missing.")

                room["realtor_id"] = realtor_id
                room["complex_id"] = complex.get("complex_id")
                room["call_number"] = contact.get("call_number")
                add_apt_detail_data(apt_df, idx, room)

            upload_to_s3(today_apt_file_name, apt_df)
            realtor_df = pd.DataFrame(realtor_infos)
            realtor_df = realtor_df.drop_duplicates(subset="id")
            realtor_df.drop(columns=["location"], inplace=True)
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            upload_to_s3(today_realtor_file_name, realtor_df)
            apt_complex_df = pd.DataFrame(apt_complex_infos)
            apt_complex_df = apt_complex_df.drop_duplicates(subset="complex_id")
            apt_complex_df.drop(columns=["is_urban_officetel"], inplace=True)
            today_complex_file_name = get_today_file_name(DABANG_APT_COMPLEX_FILE_NAME)
            upload_to_s3(today_complex_file_name, apt_complex_df)

        fetch_apt() >> extract_new_apt() >> transform_apt() >> fetch_apt_detail_and_complex_and_realtor()

    @task_group(
        group_id="transform_and_load_to_redshift_apt_real_estate",
        tooltip="아파트 매물 전처리 후 Redshift 적재"
    )
    def transform_and_load_to_redshift_real_estate():
        @task
        def prepare_apt_for_upload():
            today_apt_file_name = get_today_file_name(DABANG_APT_FILE_NAME)
            apt_df = get_csv_from_s3(today_apt_file_name)
            desired_columns = ["id", "dabang_realtor_id", "dabang_complex_id", "seq", "roomTypeName", "roomTitle",
                               "roomDesc", "priceTypeName", "priceTitle", "latitude", "longitude",
                               "building_approval_date_str", "saved_time_str", "room_floor_str", "building_floor_str",
                               "direction_str", "address", "road_address", "dong", "ho", "maintenance_cost", "memo",
                               "deal_price", "warrant_price", "rent_price", "room_size", "provision_size", "heating",
                               "bath_num", "beds_num", "hash_tags", "parking", "parking_num", "elevator_str",
                               "loan_str", "safeties", "room_options", "contact_number",
                               ]
            apt_df = apt_df[desired_columns]
            apt_df["parking_num"] = apt_df["parking_num"].fillna(0).astype(int)
            apt_df["maintenance_cost"] = apt_df["maintenance_cost"].fillna(0).astype(int)
            apt_df["contact_number"] = apt_df["contact_number"].astype(str)
            apt_df["created_at"] = datetime.today()
            apt_df["updated_at"] = datetime.today()
            today_transform_apt_file_name = get_today_file_name("transform_" + DABANG_APT_FILE_NAME)
            upload_to_s3(today_transform_apt_file_name, apt_df)

        @task
        def load_to_redshift_apt():
            today_transform_apt_file_name = get_today_file_name("transform_" + DABANG_APT_FILE_NAME)
            cur = get_redshift_connection()
            cur.execute(f"""
                        COPY {SCHEMA}.dabang_real_estate
                        FROM 's3://team-ariel-2-data/data/{today_transform_apt_file_name}'
                        IAM_ROLE '{iam_role}'
                        CSV 
                        IGNOREHEADER 1;""")
            cur.close()
            logging.info(f'Data successfully loaded into {SCHEMA}.dabang_real_estate')

        prepare_apt_for_upload() >> load_to_redshift_apt()

    @task_group(
        group_id="extract_transform_new_complex_and_load_to_redshift",
        tooltip="새로운 아파트 단지 추출하고 전처리 후 Redshift 적재"
    )
    def extract_transform_new_complex_and_load_to_redshift():
        @task.short_circuit
        def extract_new_complex():
            today_complex_file_name = get_today_file_name(DABANG_APT_COMPLEX_FILE_NAME)
            complex_df = get_csv_from_s3(today_complex_file_name)
            realtor_ids = get_all_dabang_complex_pk()
            new_complex_df = complex_df[~complex_df["complex_id"].isin(realtor_ids)]
            if new_complex_df.empty:
                return False

            upload_to_s3(today_complex_file_name, new_complex_df)
            return True

        @task
        def fetch_complex_detail():
            today_complex_file_name = get_today_file_name(DABANG_APT_COMPLEX_FILE_NAME)
            complex_df = get_csv_from_s3(today_complex_file_name)
            for idx, row in complex_df.iterrows():
                detail_json = get_complex_detail_info(row["complex_id"])
                complex_df.loc[idx, "complex_lowest_floor"] = detail_json.get("complex_lowest_floor", "")
                complex_df.loc[idx, "complex_highest_floor"] = detail_json.get("complex_highest_floor", "")
                complex_df.loc[idx, "build_cov_ratio"] = detail_json.get("build_cov_ratio", 0.0)
                complex_df.loc[idx, "floor_area_index"] = detail_json.get("floor_area_index", 0.0)
                complex_df.loc[idx, "provider_name"] = detail_json.get("provider_name", "")
                complex_df.loc[idx, "heat_type_str"] = detail_json.get("heat_type_str", "")
                complex_df.loc[idx, "fuel_type_str"] = detail_json.get("fuel_type_str", "")

                if detail_json.get("location") is not None:
                    complex_df.loc[idx, "latitude"] = detail_json.get("location")[1]
                    complex_df.loc[idx, "longitude"] = detail_json.get("location")[0]
                else:
                    complex_df.loc[idx, "latitude"] = 0.0
                    complex_df.loc[idx, "longitude"] = 0.0

            complex_df["created_at"] = datetime.today()
            complex_df["updated_at"] = datetime.today()
            desired_columns = ["complex_id", "complex_name", "household_num", "building_approval_date_str",
                               "jibun_address", "road_address", "parking_num", "parking_average", "latitude",
                               "longitude", "complex_lowest_floor", "complex_highest_floor", "build_cov_ratio",
                               "floor_area_index", "provider_name", "heat_type_str", "fuel_type_str", "created_at",
                               "updated_at",
                               ]
            complex_df = complex_df[desired_columns]
            complex_df["parking_num"] = complex_df["parking_num"].fillna(0).astype(int)
            upload_to_s3(today_complex_file_name, complex_df)

        @task
        def load_to_redshift_complex():
            today_complex_file_name = get_today_file_name(DABANG_APT_COMPLEX_FILE_NAME)
            cur = get_redshift_connection()
            cur.execute(f"""
                        COPY {SCHEMA}.dabang_complex
                        FROM 's3://team-ariel-2-data/data/{today_complex_file_name}'
                        IAM_ROLE '{iam_role}'
                        CSV 
                        IGNOREHEADER 1;""")
            cur.close()
            logging.info(f'Data successfully loaded into {SCHEMA}.dabang_complex')

        extract_new_complex() >> fetch_complex_detail() >> load_to_redshift_complex()

    @task_group(
        group_id="extract_transform_new_realtor_and_load_to_redshift",
        tooltip="새로운 공인중개사 추출 및 전처리 후 Redshift 적재"
    )
    def extract_transform_new_realtor_and_load_to_redshift():
        @task.short_circuit
        def extract_new_realtor():
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            realtor_df = get_csv_from_s3(today_realtor_file_name)
            realtor_ids = get_all_dabang_realtor_pk()
            new_realtor_df = realtor_df[~realtor_df["id"].isin(realtor_ids)]
            if new_realtor_df.empty:
                return False

            upload_to_s3(today_realtor_file_name, new_realtor_df)
            return True

        @task
        def transform_realtor():
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            realtor_df = get_csv_from_s3(today_realtor_file_name)
            desired_columns = ["id", "name", "address", "agent_tel", "email", "facename", "latitude", "longitude",
                               "reg_id", "users_idx", "greetings"
                               ]
            realtor_df = realtor_df[desired_columns]
            realtor_df["created_at"] = datetime.today()
            realtor_df["updated_at"] = datetime.today()
            upload_to_s3(today_realtor_file_name, realtor_df)

        @task
        def load_to_redshift_realtor():
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            cur = get_redshift_connection()
            cur.execute(f"""
                        COPY {SCHEMA}.dabang_realtor
                        FROM 's3://team-ariel-2-data/data/{today_realtor_file_name}'
                        IAM_ROLE '{iam_role}'
                        CSV 
                        IGNOREHEADER 1;""")
            cur.close()
            logging.info(f'Data successfully loaded into {SCHEMA}.dabang_realtor')

        extract_new_realtor() >> transform_realtor() >> load_to_redshift_realtor()

    fetch_apt_complex_realtor = fetch_and_process_apt_complex_realtor_data()
    transform_and_load_to_redshift_apt = transform_and_load_to_redshift_real_estate()
    extract_transform_and_load_to_redshift_complex = extract_transform_new_complex_and_load_to_redshift()
    extract_transform_and_load_to_redshift_realtor = extract_transform_new_realtor_and_load_to_redshift()

    fetch_and_process_region_data() >> fetch_apt_complex_realtor
    fetch_apt_complex_realtor >> transform_and_load_to_redshift_apt
    fetch_apt_complex_realtor >> extract_transform_and_load_to_redshift_complex
    fetch_apt_complex_realtor >> extract_transform_and_load_to_redshift_realtor


dabang_apt_real_estate()
