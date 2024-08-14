import ast, re, time, requests, logging, json
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from airflow.decorators import task, dag, task_group
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

SCHEMA = "raw_data"
BUCKET_NAME = "team-ariel-2-data"
SEOUL_CORTARNO = 1100000000
PREFIX_TRANSFORM = "transform_"
SI_GUN_GU_FILE_NAME = "si_gun_gu.csv"
EUP_MYEON_DONG_FILE_NAME = "eup_myeon_dong.csv"

DABANG_ONE_ROOM_FILE_NAME = "dabang_one_room_real_estate.csv"
DABANG_REALTOR_FILE_NAME = "dabang_one_room_realtor.csv"
DABANG_ONE_ROOM_URL = "https://www.dabangapp.com/api/v5/room-list/category/one-two/region"
DABANG_ONE_ROOM_DETAIL_URL = "https://www.dabangapp.com/api/3/new-room/detail"
DABANG_ONE_ROOM_ADDRESS_URL = "https://www.dabangapp.com/api/3/room/near"
DABANG_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"}
DABANG_ONE_ROOM_HEADERS = {
    "D-Api-Version": "5.0.0",
    "D-App-Version": "1",
    "D-Call-Type": "web",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}
DABANG_ONE_ROOM_FILTERS = {
    "sellingTypeList": ["MONTHLY_RENT", "LEASE"],
    "depositRange": {"min": 0, "max": 999999},
    "priceRange": {"min": 0, "max": 999999},
    "isIncludeMaintenance": False,
    "pyeongRange": {"min": 0, "max": 999999},
    "roomFloorList": ["GROUND_FIRST", "GROUND_SECOND_OVER", "SEMI_BASEMENT", "ROOFTOP"],
    "roomTypeList": ["ONE_ROOM", "TWO_ROOM"],
    "dealTypeList": ["AGENT", "DIRECT"],
    "canParking": False,
    "isShortLease": False,
    "hasElevator": False,
    "hasPano": False,
    "isDivision": False,
    "isDuplex": False
}

NAVER_REGION_URL = "https://new.land.naver.com/api/regions/list?cortarNo="
NAVER_SEARCH_URL = "https://map.naver.com/p/api/search/allSearch"
NAVER_COORDINATE_URL = "https://map.naver.com/p/api/polygon"
NAVER_BASE_HEADERS = {
    "Accept-Encoding": "gzip",
    "Host": "new.land.naver.com",
    "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
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


def convert_to_list(location_str):
    try:
        return ast.literal_eval(location_str)
    except (ValueError, SyntaxError):
        return [None, None]


def fix_json_format(json_str):
    try:
        fixed_str = re.sub(r"(?<!\\)'", '"', json_str)
        return json.loads(fixed_str)
    except (json.JSONDecodeError, TypeError):
        return {}


def fill_missing_dabang_numeric_values(df, columns):
    df[columns] = df[columns].fillna(0)
    return df


def add_necessary_columns(df, columns):
    for column in columns:
        if column not in df.columns:
            df[column] = None


def get_today_file_name(file_name):
    today = datetime.now().strftime("%Y-%m-%d")
    return f"{today}_{file_name}"


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
        logging.error(f"Error S3 '{BUCKET_NAME} '{file_name}' is not found: {e}")
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


def get_df_from_s3_csv(file_name, dtype_spec=None):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        key = "data/" + file_name
        csv_data = s3_hook.get_key(key=key, bucket_name=BUCKET_NAME).get()["Body"]
        return pd.read_csv(csv_data, dtype=dtype_spec) if dtype_spec else pd.read_csv(csv_data)
    except FileNotFoundError as fe:
        logging.error(
            f"FileNotFoundError: The file '{file_name}' could not be found in the S3 bucket '{BUCKET_NAME}'."
            f"Check the S3 permissions as well. Detailed error: {fe}"
        )
        raise
    except Exception as e:
        logging.error(
            f"Unexpected error: An unexpected error occurred while attempting to read the CSV file from S3 at path '{key}'. "
            f"Detailed error: {e}"
        )
        raise


def get_region_info(cortar_no):
    url = f"{NAVER_REGION_URL}{cortar_no}"
    res = requests.get(url, headers=NAVER_BASE_HEADERS)
    _json = res.json()
    return pd.DataFrame(_json["regionList"])


def get_naver_search(keyword):
    params = {
        "query": keyword,
        "type": all,
        "searchCoord": f"{0};{0}",
        "boundary": ""
    }

    try:
        res = requests.get(NAVER_SEARCH_URL, params=params, headers=DABANG_BASE_HEADERS)
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
        res = requests.get(NAVER_COORDINATE_URL, params=params, headers=DABANG_BASE_HEADERS)
        res.raise_for_status()
        return res.json().json()["features"][0]["bbox"]
    except Exception as e:
        logging.error(f"Request failed: {e}; keyword_rcode: '{keyword_rcode}'")
        raise


def get_dabang_one_room(code, sw, ne):
    bbox = {
        "sw": sw,
        "ne": ne
    }
    one_room_df_list = []
    page = 1
    while True:
        params = {
            "bbox": json.dumps(bbox),
            "code": code,
            "filters": json.dumps(DABANG_ONE_ROOM_FILTERS),
            "page": page,
            "useMap": "naver",
            "zoom": "14",
        }
        res = requests.get(DABANG_ONE_ROOM_URL, params=params, headers=DABANG_ONE_ROOM_HEADERS)
        res.raise_for_status()
        one_room_json = res.json()
        one_room_df = pd.DataFrame(one_room_json["result"]["roomList"])
        one_room_df_list.append(one_room_df)
        if not one_room_json["result"]["hasMore"]:
            break

        page += 1

    return pd.concat(one_room_df_list, ignore_index=True)


def get_all_dabang_one_room_pk():
    cur = get_redshift_connection()
    cur.execute(f"SELECT id FROM {SCHEMA}.dabang_real_estate")
    results = cur.fetchall()
    cur.close()
    return [row[0] for row in results]


def get_dabang_one_room_address(room_id):
    params = {
        "api_version": "3.0.1",
        "call_type": "web",
        "room_id": room_id,
        "version": "1"
    }
    res = requests.get(DABANG_ONE_ROOM_ADDRESS_URL, params=params, headers=DABANG_BASE_HEADERS)
    res.raise_for_status()
    near_json = res.json()
    return near_json['address']


def get_dabang_one_room_detail(room_id):
    params = {
        "api_version": "3.0.1",
        "call_type": "web",
        "room_id": room_id,
        "version": "1"
    }
    try:
        res = requests.get(DABANG_ONE_ROOM_DETAIL_URL, params=params, headers=DABANG_BASE_HEADERS)
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


def int_comversion(value):
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except (ValueError, TypeError):
        return 0


def add_one_room_detail_data(one_room_df, idx, room):
    try:
        one_room_df.loc[idx, "address"] = room.get("address", "")
        one_room_df.loc[idx, "building_use_types_str"] = room.get("building_use_types_str")[0] if room.get(
            "building_use_types_str") else None
        one_room_df.at[idx, "contact_number"] = room.get("call_number", "")
        one_room_df.at[idx, "dabang_realtor_id"] = room.get("realtor_id", "")
        one_room_df.at[idx, "memo"] = room.get("memo", "")
        one_room_df.loc[idx, "dong"] = room.get("dong")
        one_room_df.at[idx, "ho"] = room.get("ho", "")
        one_room_df.at[idx, "saved_time_str"] = room.get("saved_time_str", "")
        one_room_df.loc[idx, "roomTypeName"] = room.get("room_type_str")
        one_room_df.at[idx, "heating"] = room.get("heating", "")
        one_room_df.at[idx, "room_floor_str"] = room.get("room_floor_str", "")
        one_room_df.at[idx, "building_floor_str"] = room.get("building_floor_str", "")
        one_room_df.at[idx, "building_approval_date_str"] = room.get("building_approval_date_str", "")
        one_room_df.loc[idx, "bath_num"] = int_comversion(room.get("bath_num"))
        one_room_df.loc[idx, "beds_num"] = int_comversion(room.get("beds_num"))
        one_room_df.loc[idx, "maintenance_cost"] = int_comversion(room.get("maintenance_cost"))
        one_room_df.at[idx, "hash_tags"] = room.get("hash_tags", "")
        one_room_df.loc[idx, "parking"] = room.get("parking")
        one_room_df.loc[idx, "parking_num"] = room.get("parking_num")
        one_room_df.loc[idx, "elevator_str"] = room.get("elevator_str")
        one_room_df.loc[idx, "loan_str"] = room.get("loan_str")
        one_room_df.loc[idx, "direction_str"] = room.get("direction_str")
        one_room_df.loc[idx, "room_size"] = room.get("room_size")  # 전용면적
        one_room_df.loc[idx, "provision_size"] = room.get("provision_size")  # 공급면적
        one_room_df.loc[idx, "rent_price"] = room.get("price_info")[0][1]

        price_info = room.get("price_info")
        if price_info[0][2] == 2:
            one_room_df.loc[idx, "deal_price"] = price_info[0][0]
        else:
            one_room_df.loc[idx, "warrant_price"] = price_info[0][0]

        if room.get("room_options") is not None:
            room_option_names = [item["name"] for item in room.get("room_options")]
            one_room_df.at[idx, "room_options"] = room_option_names

        if room.get("safeties") is not None:
            safeties_names = [item["name"] for item in room.get("safeties")]
            one_room_df.at[idx, "safeties"] = safeties_names
    except Exception as e:
        logging.error(f"Failed to process room data for index {idx} with error: {e}; room data: {room}")
        raise


def get_all_dabang_realtor_pk():
    cur = get_redshift_connection()
    cur.execute(f"SELECT id FROM {SCHEMA}.dabang_realtor")
    resutls = cur.fetchall()
    cur.close()
    return [row[0] for row in resutls]


@dag(
    default_args=default_args,
    description="다방 원룸 매물, 공인중개사 데이터 수집 및 적재 DAG",
    schedule_interval="0 14 * * *",
    start_date=datetime(2024, 8, 11),
    catchup=False,
    tags=["daily", "real_estate", "dabang", "one_room"]
)
def dabang_one_room_real_estate():
    @task_group(
        group_id="fetch_and_process_coordinate_region_data",
        tooltip="서울 전지역 읍/면/동 S3 업로드"
    )
    def fetch_and_process_coordinate_region_data():
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
            si_gun_gu_df = get_df_from_s3_csv(SI_GUN_GU_FILE_NAME)
            for cortarNo in si_gun_gu_df['cortarNo']:
                eup_myeon_dong_df = get_region_info(cortarNo)
                eup_myeon_dong_df_list.append(eup_myeon_dong_df)

            eup_myeon_dong_df = pd.concat(eup_myeon_dong_df_list, ignore_index=True)
            upload_to_s3(EUP_MYEON_DONG_FILE_NAME, eup_myeon_dong_df)

        @task
        def add_coordinate_to_eup_myeon_dong():
            dong_df = get_df_from_s3_csv(EUP_MYEON_DONG_FILE_NAME)
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
        group_id="fetch_one_room_and_realtor_data",
        tooltip="새로운 원룸 매물, 공인중개사 S3 업로드"
    )
    def fetch_one_room_and_realtor_data():
        @task
        def fetch_one_room():
            one_room_df_list = []
            dong_df = get_df_from_s3_csv(EUP_MYEON_DONG_FILE_NAME)
            for idx, row in dong_df.iterrows():
                sw = {"lat": row["sw_lat"], "lng": row["sw_lng"]}
                ne = {"lat": row["ne_lat"], "lng": row["ne_lng"]}
                code = int(str(row["cortarNo"])[:-2])
                one_room_df = get_dabang_one_room(code, sw, ne)
                one_room_df_list.append(one_room_df)

            one_room_df = pd.concat(one_room_df_list, ignore_index=True)
            today_one_room_file_name = get_today_file_name(DABANG_ONE_ROOM_FILE_NAME)
            upload_to_s3(today_one_room_file_name, one_room_df)

        @task.short_circuit
        def extract_new_one_room():
            today_one_room_file_name = get_today_file_name(DABANG_ONE_ROOM_FILE_NAME)
            one_room_df = get_df_from_s3_csv(today_one_room_file_name)
            one_room_ids = get_all_dabang_one_room_pk()
            new_one_room_df = one_room_df[~one_room_df["id"].isin(one_room_ids)]
            if new_one_room_df.empty:
                return False

            upload_to_s3(today_one_room_file_name, new_one_room_df)
            return True

        @task
        def transform_new_one_room():
            today_one_room_file_name = get_today_file_name(DABANG_ONE_ROOM_FILE_NAME)
            one_room_df = get_df_from_s3_csv(today_one_room_file_name)
            desired_columns = ['seq', 'id', 'roomTypeName', 'randomLocation', 'roomTitle', 'roomDesc', 'priceTypeName',
                               'priceTitle']
            one_room_df = one_room_df[desired_columns]
            necessary_columns = ["complexName", "latitude", "longitude", "address", "road_address", "bath_num",
                                 "beds_num", "building_approval_date_str", "building_floor_str",
                                 "building_use_types_str", "dabang_complex_id", "dong", "ho", "direction_str",
                                 "hash_tags", "maintenance_cost", "memo", "parking", "parking_num", "elevator_str",
                                 "loan_str", "deal_price", "warrant_price", "rent_price", "provision_size", "room_size",
                                 "saved_time_str", "room_options", "safeties", "contact_number", "dabang_realtor_id",
                                 "heating", "room_floor_str",
                                 ]
            add_necessary_columns(one_room_df, necessary_columns)
            numeric_columns = [
                "latitude", "longitude", "maintenance_cost", "deal_price",
                "warrant_price", "rent_price", "room_size", "provision_size",
                "bath_num", "beds_num", "parking_num"
            ]
            one_room_df = fill_missing_dabang_numeric_values(one_room_df, numeric_columns)
            one_room_df["randomLocation"] = one_room_df["randomLocation"].apply(fix_json_format)
            one_room_df.loc[:, "latitude"] = one_room_df["randomLocation"].apply(lambda x: x["lat"])
            one_room_df.loc[:, "longitude"] = one_room_df["randomLocation"].apply(lambda x: x["lng"])
            one_room_df.drop(columns=["randomLocation"], inplace=True)
            today_transform_one_room_file_name = get_today_file_name(PREFIX_TRANSFORM + DABANG_ONE_ROOM_FILE_NAME)
            upload_to_s3(today_transform_one_room_file_name, one_room_df)

        @task
        def fetch_one_room_detail_and_realtor():
            realtor_infos = []
            today_transform_one_room_file_name = get_today_file_name(PREFIX_TRANSFORM + DABANG_ONE_ROOM_FILE_NAME)
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
            one_room_df = get_df_from_s3_csv(today_transform_one_room_file_name, dtype_spec)
            for idx, row in one_room_df.iterrows():
                detail_json = get_dabang_one_room_detail(row["id"])
                if detail_json is None:
                    continue

                room = detail_json["room"]
                realtor = detail_json["agent"]
                contact = detail_json["contact"]
                realtor_id = ""
                if realtor is not None:
                    realtor_infos.append(realtor)
                    realtor_id = realtor.get("id", "")

                room["address"] = get_dabang_one_room_address(row["id"])
                room["realtor_id"] = realtor_id
                room["call_number"] = contact.get("call_number")
                room["building_use_types_str"] = contact.get("building_use_types_str")
                add_one_room_detail_data(one_room_df, idx, room)

            upload_to_s3(today_transform_one_room_file_name, one_room_df)
            realtor_df = pd.DataFrame(realtor_infos)
            realtor_df = realtor_df.drop_duplicates(subset="id")
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            upload_to_s3(today_realtor_file_name, realtor_df)

        fetch_one_room() >> extract_new_one_room() >> transform_new_one_room() >> fetch_one_room_detail_and_realtor()

    @task_group(
        group_id="transform_and_load_to_redshift_one_room",
        tooltip="원룸 매물 전처리 후 Redshift 적재"
    )
    def transform_and_load_to_redshift_one_room():
        @task
        def transform_one_room():
            today_transform_one_room_file_name = get_today_file_name(PREFIX_TRANSFORM + DABANG_ONE_ROOM_FILE_NAME)
            one_room_df = get_df_from_s3_csv(today_transform_one_room_file_name)
            desired_columns = ["id", "dabang_realtor_id", "dabang_complex_id", "seq", "roomTypeName", "roomTitle",
                               "roomDesc", "priceTypeName", "priceTitle", "latitude", "longitude",
                               "building_approval_date_str", "saved_time_str", "room_floor_str", "building_floor_str",
                               "direction_str", "address", "road_address", "dong", "ho", "maintenance_cost", "memo",
                               "deal_price", "warrant_price", "rent_price", "room_size", "provision_size", "heating",
                               "bath_num", "beds_num", "hash_tags", "parking", "parking_num", "elevator_str",
                               "loan_str", "safeties", "room_options", "contact_number",
                               ]
            one_room_df = one_room_df[desired_columns]
            one_room_df["parking_num"] = one_room_df["parking_num"].fillna(0).astype(int)
            one_room_df["contact_number"] = one_room_df["contact_number"].astype(str)
            one_room_df["created_at"] = datetime.today()
            one_room_df["updated_at"] = datetime.today()
            upload_to_s3(today_transform_one_room_file_name, one_room_df)

        @task
        def load_to_redshift_one_room():
            today_transform_one_room_file_name = get_today_file_name(PREFIX_TRANSFORM + DABANG_ONE_ROOM_FILE_NAME)
            cur = get_redshift_connection()
            cur.execute(f"""
                        COPY {SCHEMA}.dabang_real_estate
                        FROM 's3://team-ariel-2-data/data/{today_transform_one_room_file_name}'
                        IAM_ROLE '{iam_role}'
                        CSV 
                        IGNOREHEADER 1;""")
            cur.close()
            logging.info(f'Data successfully loaded into {SCHEMA}.dabang_real_estate')

        transform_one_room() >> load_to_redshift_one_room()

    @task_group(
        group_id="extract_transform_load_to_redshift_realtor",
        tooltip="새로운 공인중개사 추출 및 전처리 후 Redshift 적재"
    )
    def extract_transform_load_to_redshift_realtor():
        @task.short_circuit
        def extract_new_realtor():
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            realtor_df = get_df_from_s3_csv(today_realtor_file_name)
            realtor_ids = get_all_dabang_realtor_pk()
            new_realtor_df = realtor_df[~realtor_df["id"].isin(realtor_ids)]
            if new_realtor_df.empty:
                return False

            upload_to_s3(today_realtor_file_name, realtor_df)
            return True

        @task
        def transform_new_realtor():
            today_realtor_file_name = get_today_file_name(DABANG_REALTOR_FILE_NAME)
            realtor_df = get_df_from_s3_csv(today_realtor_file_name)
            realtor_df['location'] = realtor_df['location'].apply(convert_to_list)
            realtor_df['longitude'] = realtor_df['location'].apply(lambda x: x[0] if x else None)
            realtor_df['latitude'] = realtor_df['location'].apply(lambda x: x[1] if x else None)
            desired_columns = ["id", "name", "address", "agent_tel", "email", "facename", "latitude", "longitude",
                               "reg_id", "users_idx", "greetings"
                               ]
            realtor_df = realtor_df[desired_columns]
            realtor_df["created_at"] = datetime.today()
            realtor_df["updated_at"] = datetime.today()
            today_transform_realtor_file_name = get_today_file_name(PREFIX_TRANSFORM + DABANG_REALTOR_FILE_NAME)
            upload_to_s3(today_transform_realtor_file_name, realtor_df)

        @task
        def load_to_redshift_realtor():
            today_transform_realtor_file_name = get_today_file_name(PREFIX_TRANSFORM + DABANG_REALTOR_FILE_NAME)
            cur = get_redshift_connection()
            cur.execute(f"""
                        COPY {SCHEMA}.dabang_realtor
                        FROM 's3://team-ariel-2-data/data/{today_transform_realtor_file_name}'
                        IAM_ROLE '{iam_role}'
                        CSV 
                        IGNOREHEADER 1;""")
            cur.close()
            logging.info(f'Data successfully loaded into {SCHEMA}.dabang_realtor')

        extract_new_realtor() >> transform_new_realtor() >> load_to_redshift_realtor()

    fetch_one_room_and_realtor = fetch_one_room_and_realtor_data()
    fetch_and_process_coordinate_region_data() >> fetch_one_room_and_realtor
    fetch_one_room_and_realtor >> transform_and_load_to_redshift_one_room()
    fetch_one_room_and_realtor >> extract_transform_load_to_redshift_realtor()


dabang_one_room_real_estate()
