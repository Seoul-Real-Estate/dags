import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

import requests
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

SCHEMA = "raw_data"
BUCKET_NAME = "team-ariel-2-data"
PREFIX_INTEGRATE = "integrate_"
PREFIX_TRANSFORM = "transform_"
PREFIX_DATA = "data/"
NAVER_REAL_ESTATE_FILE_NAME = "naver_real_estate.csv"
DABANG_REAL_ESTATE_FILE_NAME = "dabang_real_estate.csv"
VWORLD_URL = "https://api.vworld.kr/req/address?"
DEFAULT_ARGS = {
    "owner": "yong",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

iam_role = Variable.get("aws_iam_role")


def get_today_file_name(file_name):
    today = datetime.today().strftime("%Y-%m-%d")
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


def get_s3_hook():
    try:
        return S3Hook(aws_conn_id="aws_s3_connection")
    except Exception as e:
        logging.error(f"Error S3 Hook: '{e}'")


def upload_to_s3(file_name, data_frame):
    try:
        s3_hook = get_s3_hook()
        csv_buffer = StringIO()
        key = PREFIX_DATA + file_name
        data_frame.to_csv(csv_buffer, index=False)
        s3_hook.load_string(string_data=csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        logging.info(f"Upload to S3 {BUCKET_NAME}: {file_name}")
    except Exception as e:
        logging.error(f"Error Upload to S3 {BUCKET_NAME}: {e}")
        raise


def get_df_from_s3_csv(file_name, dtype_spec=None):
    try:
        s3_hook = get_s3_hook()
        key = PREFIX_DATA + file_name
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


def get_new_dabang_real_estate():
    cur = get_redshift_connection()
    query = f"""
    SELECT 
        dr.id as id, 
        dr.dabang_complex_id as complex_id, 
        dr.dabang_realtor_id as realtor_id, 
        CASE
            WHEN dr.roomtypename LIKE '쓰리룸%' THEN '빌라'
            WHEN dr.roomtypename LIKE '원룸%' OR dr.roomtypename LIKE '투룸%' THEN '원룸'
            WHEN dr.roomtypename LIKE '아파트%' OR dr.roomtypename LIKE '오피스텔%' THEN dc.complex_name
        END as room_name,
        dr.roomtypename as room_type, 
        dr.pricetypename as trade_type, 
        dr.room_floor_str as room_floor, 
        dr.building_floor_str as building_floor, 
        dr.room_size as supply_area,
        dr.provision_size as exclusive_area,
        dr.direction_str as direction,
        dr.deal_price as deal_price,
        dr.warrant_price as warrant_price,
        dr.rent_price as rent_price,
        dr.roomtitle as room_title,
        dr.memo as description,
        dr.hash_tags as hash_tags,
        dr.latitude as latitude,
        dr.longitude as longitude,
        '' as cortar_no,
        NULL as region_gu,
        NULL as region_dong,
        dr.address as address,
        dr.road_address as road_address,
        '' as etc_address,
        dr.ho as ho_num,
        dr.dong as dong_num,
        dr.parking_num as parking_count,
        dr.parking as is_parking,
        dr.beds_num as room_count,
        dr.bath_num as bath_count,
        dr.room_options as room_options,
        dr.safeties as safe_options,
        dr.building_approval_date_str as approve_date,
        dr.saved_time_str as expose_start_date,
        dr.heating as heat_type,
        dc.fuel_type_str as heat_fuel_type,
        dc.complex_name as complex_name,
        dc.jibun_address as complex_address,
        dc.road_address as complex_road_address
    FROM {SCHEMA}.dabang_real_estate dr
    LEFT JOIN {SCHEMA}.dabang_complex dc
        ON dr.dabang_complex_id = dc.complex_id
    LEFT JOIN {SCHEMA}.real_estate re
        ON re.id = dr.id AND re.platform = 'dabang'
    WHERE re.id is NULL
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    return df


def get_new_naver_real_estate():
    cur = get_redshift_connection()
    query = f"""
    SELECT 
        nr.articleno as id, 
        nr.complexno as complex_id, 
        nr.realtorid as realtor_id, 
        CASE 
            WHEN nr.realestatetypename LIKE '아파트%' THEN nr.articlename
            WHEN nr.realestatetypename LIKE '오피스텔%' THEN nr.articlename
            ELSE nr.realestatetypename 
        END as room_name,
        nr.realestatetypename as room_type, 
        nr.tradetypename as trade_type, 
        nr.correspondingfloorcount as room_floor, 
        nr.totalfloorcount as building_floor, 
        nr.supply_area as supply_area,
        nr.exclusive_area as exclusive_area,
        nr.direction as direction,
        nr.dealprice as deal_price,
        nr.warrantprice as warrant_price,
        nr.rentprice as rent_price,
        nr.articlefeaturedesc as room_title,
        nr.detaildescription as description,
        nr.taglist as hash_tags,
        nr.latitude as latitude,
        nr.longitude as longitude,
        '' as cortar_no,
        '' as region_gu,
        '' as region_dong,
        nr.exposureaddress as address,
        nr.roadaddress as road_address,
        nr.etcaddress as etc_address,
        nr.honm as ho_num,
        nr.buildingname as dong_num,
        nr.parkingcount as parking_count,
        nr.parkingpossibleyn as is_parking,
        nr.roomcount as room_count,
        nr.bathroomcount as bath_count,
        nr.roomfacilities as room_options,
        nr.buildingfacilities as safe_options,
        nr.useapproveymd as approve_date,
        nr.exposestartymd as expose_start_date,
        nr.heatmethodtypename as heat_type,
        nr.heatfueltypename as heat_fuel_type,
        nc.complexname as complex_name,
        nc.address as complex_address,
        nc.road_address as complex_road_address
    FROM {SCHEMA}.naver_real_estate nr
    LEFT JOIN {SCHEMA}.naver_complex nc
        ON nr.complexno = nc.complexno
    LEFT JOIN {SCHEMA}.real_estate re
        ON re.id = nr.articleno AND re.platform = 'naver'
    WHERE re.id IS NULL
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    return df


def convert_int_to_str(value):
    try:
        return str(int(float(value)))
    except ValueError:
        return str(value)
    except Exception as e:
        logging.error(f"Error value '{value}' could not convert int to str: '{e}'")
        raise


def process_columns(df, columns, processor, **kwargs):
    for column in columns:
        df = processor(df, column, **kwargs)
    return df


def process_numeric_column(df, column_name, dtype="float"):
    df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0).astype(dtype)
    return df


def process_date_column(df, column_name, date_format="%Y%m%d"):
    df[column_name] = pd.to_datetime(df[column_name].fillna(0).astype(int).astype(str), errors="coerce",
                                     format=date_format)
    return df


def process_dabang_date_column(df, column_name, date_format="%Y.%m.%d"):
    df[column_name] = df[column_name].apply(lambda x: f"{x}.01" if isinstance(x, str) and len(x) == 7 else x)
    df[column_name] = pd.to_datetime(df[column_name].astype(str), errors="coerce", format=date_format)
    return df


def process_convert_int_to_str(df, column_name):
    df[column_name] = df[column_name].fillna("-").apply(convert_int_to_str).astype(str)
    return df


def fill_null_with_dash(df, columns):
    for column in columns:
        df[column] = df[column].fillna("-")
    return df


def transform_apt_address(df):
    mask = df["room_type"].isin(["아파트", "오피스텔"])
    df.loc[mask, "address"] = df.loc[mask, "complex_address"]
    df.loc[mask, "road_address"] = df.loc[mask, "complex_road_address"]
    df["complex_name"] = df["complex_name"].fillna("").astype(str)
    df["dong_num"] = df["dong_num"].str.replace("-", "", regex=False).astype(str)
    df["dong_num"] = df["dong_num"].str.rstrip("동")
    df["ho_num"] = df["ho_num"].str.replace("-", "", regex=False).astype(str)
    df["ho_num"] = df["ho_num"].str.rstrip("호")
    df["etc_address"] = np.where(
        mask,
        df["complex_name"] + " " +
        np.where(df["dong_num"] != "", df["dong_num"] + "동 ", "") +
        np.where(df["ho_num"] != "", df["ho_num"] + "호", ""),
        ""
    )

    return df


def transform_dabang_apt_address(df):
    mask = df["room_type"].isin(["아파트", "오피스텔"])
    df.loc[mask, "address"] = df.loc[mask, "complex_address"]
    df.loc[mask, "road_address"] = df.loc[mask, "complex_road_address"]
    df["complex_name"] = df["complex_name"].fillna("").astype(str)
    df["dong_num"] = df["dong_num"].str.replace("-", "", regex=False).astype(str)
    df["dong_num"] = df["dong_num"].str.rstrip("동")
    df["ho_num"] = df["ho_num"].str.replace("-", "", regex=False).astype(str)
    df["ho_num"] = df["ho_num"].str.rstrip("호")
    df["etc_address"] = np.where(
        mask,
        df["complex_name"] + " " +
        np.where(df["dong_num"] != "", df["dong_num"] + "동 ", "") +
        np.where(df["ho_num"] != "", df["ho_num"] + "호", ""),
        ""
    )

    return df


def get_count_real_estate():
    cur = get_redshift_connection()
    cur.execute(f"SELECT COUNT(1) FROM {SCHEMA}.real_estate")
    return cur.fetchone()[0]


def get_coordinate_convert_address(latitude, longitude):
    params = {
        "service": "address",
        "request": "getaddress",
        "crs": "epsg:4326",
        "point": f"{longitude},{latitude}",
        "format": "json",
        "type": "parcel",
        "key": Variable.get("vworld_key")
    }
    try:
        res = requests.get(VWORLD_URL, params=params)
        res.raise_for_status()
        result = res.json()["response"]["result"][0]
        structure = result.get("structure")
        return {
            "address": result.get("text", ""),
            "region_gu": structure.get("level2", ""),
            "region_dong": structure.get("level4L", ""),
            "cortar_no": structure.get("level4LC", ""),
            "room_name": structure.get("detail", "")
        }
    except requests.exceptions.HTTPError as he:
        logging.error(f"Error '{he.response}' message: '{res.text}' error: '{he}'")
        return None
    except KeyError as ke:
        logging.error(f"Error KeyError {ke}")
        return None
    except Exception as e:
        logging.warning(f"Error [get_apt_detail_info] message:'{e}'")
        raise


@dag(
    default_args=DEFAULT_ARGS,
    description="네이버부동산 매물과 다방 매물 통합시키는 DAG",
    schedule_interval="0 20 * * *",
    start_date=datetime(2024, 8, 12),
    catchup=False,
    tags=["daily", "real_estate", "integrate"]
)
def integrate_real_estate():
    @task.short_circuit
    def fetch_naver_new_real_estate():
        new_naver_real_estate_df = get_new_naver_real_estate()
        if new_naver_real_estate_df.empty:
            return False

        today_naver_file_name = get_today_file_name(PREFIX_INTEGRATE + NAVER_REAL_ESTATE_FILE_NAME)
        upload_to_s3(today_naver_file_name, new_naver_real_estate_df)
        return True

    @task
    def transform_naver_new_real_estate():
        today_naver_file_name = get_today_file_name(PREFIX_INTEGRATE + NAVER_REAL_ESTATE_FILE_NAME)
        naver_df = get_df_from_s3_csv(today_naver_file_name)
        for idx, row in naver_df.iterrows():
            _json = get_coordinate_convert_address(row["latitude"], row["longitude"])
            if _json:
                naver_df.loc[idx, "address"] = _json.get("address")
                naver_df.loc[idx, "room_name"] = _json.get("room_name")
                naver_df.loc[idx, "region_gu"] = _json.get("region_gu")
                naver_df.loc[idx, "region_dong"] = _json.get("region_dong")
                naver_df.loc[idx, "cortar_no"] = _json.get("cortar_no")

        str_columns = ["id", "complex_id", "realtor_id", "room_name", "room_type", "trade_type", "room_floor",
                       "building_floor", "direction", "room_title", "description", "hash_tags", "region_gu",
                       "region_dong", "address", "road_address", "etc_address", "is_parking", "heat_type",
                       "heat_fuel_type"]
        naver_df = fill_null_with_dash(naver_df, str_columns)
        naver_df["room_options"] = naver_df["room_options"].fillna("[]")
        naver_df["safe_options"] = naver_df["safe_options"].fillna("[]")
        int_to_str_columns = ["building_floor", "room_floor", "complex_id", "ho_num", "dong_num"]
        naver_df = process_columns(naver_df, int_to_str_columns, process_convert_int_to_str)
        numeric_columns = ["supply_area", "exclusive_area"]
        naver_df = process_columns(naver_df, numeric_columns, process_numeric_column)
        price_columns = ["deal_price", "warrant_price", "rent_price"]
        naver_df = process_columns(naver_df, price_columns, process_numeric_column, dtype="int")
        date_columns = ["approve_date", "expose_start_date"]
        naver_df = process_columns(naver_df, date_columns, process_date_column)
        naver_df = transform_apt_address(naver_df)
        naver_df.drop(columns=["complex_name", "complex_address", "complex_road_address"], inplace=True)
        naver_df["platform"] = "naver"
        naver_df["created_at"] = datetime.now()
        naver_df["updated_at"] = datetime.now()
        today_transform_naver_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + NAVER_REAL_ESTATE_FILE_NAME)
        upload_to_s3(today_transform_naver_file_name, naver_df)

    @task
    def load_to_redshift_naver_new_real_estate():
        today_transform_naver_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + NAVER_REAL_ESTATE_FILE_NAME)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {SCHEMA}.real_estate
                    FROM 's3://team-ariel-2-data/data/{today_transform_naver_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV 
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {SCHEMA}.real_estate')

    @task.short_circuit
    def fetch_dabang_new_real_estate():
        new_dabang_real_estate_df = get_new_dabang_real_estate()
        if new_dabang_real_estate_df.empty:
            return False

        today_dabang_file_name = get_today_file_name(PREFIX_INTEGRATE + DABANG_REAL_ESTATE_FILE_NAME)
        upload_to_s3(today_dabang_file_name, new_dabang_real_estate_df)
        return True

    @task
    def transform_dabang_new_real_estate():
        today_dabang_file_name = get_today_file_name(PREFIX_INTEGRATE + DABANG_REAL_ESTATE_FILE_NAME)
        dabang_df = get_df_from_s3_csv(today_dabang_file_name)
        for idx, row in dabang_df.iterrows():
            _json = get_coordinate_convert_address(row["latitude"], row["longitude"])
            if _json:
                dabang_df.loc[idx, "address"] = _json.get("address")
                dabang_df.loc[idx, "room_name"] = _json.get("room_name")
                dabang_df.loc[idx, "region_gu"] = _json.get("region_gu")
                dabang_df.loc[idx, "region_dong"] = _json.get("region_dong")
                dabang_df.loc[idx, "cortar_no"] = _json.get("cortar_no")

        str_columns = ["id", "complex_id", "realtor_id", "room_name", "room_type", "trade_type", "room_floor",
                       "building_floor", "direction", "room_title", "description", "hash_tags", "region_gu",
                       "region_dong", "address", "road_address", "etc_address", "is_parking", "heat_type",
                       "heat_fuel_type"]
        dabang_df = fill_null_with_dash(dabang_df, str_columns)
        dabang_df["room_options"] = dabang_df["room_options"].fillna("[]")
        dabang_df["safe_options"] = dabang_df["safe_options"].fillna("[]")
        int_to_str_columns = ["building_floor", "room_floor", "complex_id", "ho_num", "dong_num"]
        dabang_df = process_columns(dabang_df, int_to_str_columns, process_convert_int_to_str)
        numeric_columns = ["supply_area", "exclusive_area"]
        dabang_df = process_columns(dabang_df, numeric_columns, process_numeric_column)
        price_columns = ["deal_price", "warrant_price", "rent_price"]
        dabang_df = process_columns(dabang_df, price_columns, process_numeric_column, dtype="int")
        date_columns = ["approve_date", "expose_start_date"]
        dabang_df = process_columns(dabang_df, date_columns, process_dabang_date_column, date_format="%Y.%m.%d")
        dabang_df = transform_dabang_apt_address(dabang_df)
        dabang_df.drop(columns=["complex_name", "complex_address", "complex_road_address"], inplace=True)
        dabang_df["platform"] = "dabang"
        dabang_df["created_at"] = datetime.now()
        dabang_df["updated_at"] = datetime.now()
        today_transform_dabang_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + DABANG_REAL_ESTATE_FILE_NAME)
        upload_to_s3(today_transform_dabang_file_name, dabang_df)

    @task
    def load_to_redshift_dabang_new_real_estate():
        today_transform_dabang_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + DABANG_REAL_ESTATE_FILE_NAME)
        cur = get_redshift_connection()
        cur.execute(f"""
                            COPY {SCHEMA}.real_estate
                            FROM 's3://team-ariel-2-data/data/{today_transform_dabang_file_name}'
                            IAM_ROLE '{iam_role}'
                            CSV 
                            IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {SCHEMA}.real_estate')

    fetch_naver_new_real_estate() >> transform_naver_new_real_estate() >> load_to_redshift_naver_new_real_estate()
    fetch_dabang_new_real_estate() >> transform_dabang_new_real_estate() >> load_to_redshift_dabang_new_real_estate()


integrate_real_estate()
