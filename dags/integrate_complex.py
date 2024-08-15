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
BATCH_SIZE = 1000
PREFIX_INTEGRATE = "integrate_"
PREFIX_TRANSFORM = "transform_"
PREFIX_DATA = "data/"
NAVER_COMPLEX_FILE_NAME = "naver_complex.csv"
DABANG_COMPLEX_FILE_NAME = "dabang_complex.csv"
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


def get_new_naver_complex():
    cur = get_redshift_connection()
    query = f"""
    SELECT
        nc.complexno as id,
        nc.complexname as name,
        nc.totalhouseholdcount as household_num,
        nc.address as address,
        nc.road_address as road_address,
        nc.latitude as latitude,
        nc.longitude as longitude,
        nc.parkingpossiblecount as parking_count,
        nc.useapproveymd as approve_date,
        nc.constructionCompanyName as provider_name,
        CASE 
            WHEN nc.heatmethodtypecode = 'HT001' THEN '개별난방'
            WHEN nc.heatmethodtypecode = 'HT002' THEN '중앙난방'
            WHEN nc.heatmethodtypecode = 'HT003' THEN '개별냉난방'
            WHEN nc.heatmethodtypecode = 'HT005' THEN '지역난방'
            ELSE nc.heatmethodtypecode
        END as heat_type,
        CASE
            WHEN nc.heatfueltypecode = 'HF001' THEN '도시가스'
            WHEN nc.heatfueltypecode = 'HF002' THEN '열병합'
            WHEN nc.heatfueltypecode = 'HF003' THEN '기름'
            WHEN nc.heatfueltypecode = 'HF004' THEN '전기'
            WHEN nc.heatfueltypecode = 'HF005' THEN '심야전기'
            WHEN nc.heatfueltypecode = 'HF006' THEN '태양열'
            WHEN nc.heatfueltypecode = 'HF007' THEN 'LPG'
            ELSE nc.heatfueltypecode
        END as heat_fuel_type,
        'naver' as platform,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        '-' as cortar_no,
        '-' as region_gu,
        '-' as region_dong
    FROM {SCHEMA}.naver_complex nc
    LEFT JOIN {SCHEMA}.complex c
        ON nc.complexno = c.id AND c.platform = 'naver'
    WHERE c.id IS NULL
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    return df


def get_new_dabang_complex():
    cur = get_redshift_connection()
    query = f"""
    SELECT
        dc.complex_id as id,
        dc.complex_name as name,
        dc.household_num as household_num,
        dc.jibun_address as address,
        dc.road_address as road_address,
        dc.latitude as latitude,
        dc.longitude as longitude,
        dc.parking_num as parking_count,
        dc.building_approval_date_str as approve_date,
        dc.provider_name as provider_name,
        dc.heat_type_str as heat_type,
        dc.fuel_type_str as heat_fuel_type,
        'dabang' as platform,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        '-' as cortar_no,
        '-' as region_gu,
        '-' as region_dong
    FROM {SCHEMA}.dabang_complex dc
    LEFT JOIN {SCHEMA}.complex c
        ON dc.complex_id = c.id AND c.platform = 'dabang'
    WHERE c.id IS NULL
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    return df


def get_filtered_region_all_complex():
    cur = get_redshift_connection()
    query = f"""
    SELECT id, latitude, longitude
    FROM {SCHEMA}.complex
    WHERE region_gu = '-'
    AND region_dong = '-' 
    AND latitude != 0.0
    AND longitude != 0.0
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    return df


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
            "region_gu": structure.get("level2", ""),
            "region_dong": structure.get("level4L", ""),
            "cortar_no": structure.get("level4LC", ""),
        }
    except requests.exceptions.HTTPError as he:
        logging.error(f"Error '{he.response}' message: '{res.text}' error: '{he}'")
        return None
    except KeyError as ke:
        logging.error(f"Error KeyError {ke}, latitude: {latitude}, longitude: {longitude}")
        return None
    except Exception as e:
        logging.warning(f"Error [get_apt_detail_info] message:'{e}'")
        raise


def update_real_estate_batch_region(records):
    cur = get_redshift_connection()
    query = f"""
        UPDATE {SCHEMA}.complex
    SET 
        region_gu = %s, 
        region_dong = %s, 
        cortar_no = %s
    WHERE id = %s
    """
    try:
        cur.executemany(query, records)
        logging.info(f'Data successfully batch update into {SCHEMA}.real_estate')
    except Exception as e:
        logging.error(f"Error Update {SCHEMA}.real_estate query: '{query}' | params: '{records}'"
                      f"Error : {e}")
        raise
    finally:
        cur.close()


@dag(
    default_args=DEFAULT_ARGS,
    description="네이버부동산 아파트 단지와 다방 아파트 단지 통합시키는 DAG",
    schedule_interval="0 20 * * *",
    start_date=datetime(2024, 8, 14),
    catchup=False,
    tags=["daily", "real_estate", "integrate", "complex"]
)
def integrate_complex():
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def fetch_new_naver_complex():
        new_naver_complex_df = get_new_naver_complex()
        if new_naver_complex_df.empty:
            return False

        today_naver_complex_file_name = get_today_file_name(PREFIX_INTEGRATE + NAVER_COMPLEX_FILE_NAME)
        upload_to_s3(today_naver_complex_file_name, new_naver_complex_df)
        return True

    @task
    def transform_new_naver_complex():
        today_naver_complex_file_name = get_today_file_name(PREFIX_INTEGRATE + NAVER_COMPLEX_FILE_NAME)
        naver_df = get_df_from_s3_csv(today_naver_complex_file_name)
        naver_df["approve_date"] = pd.to_datetime(naver_df["approve_date"].fillna(0).astype(int).astype(str),
                                                  errors="coerce",
                                                  format="%Y%m%d")
        naver_df["created_at"] = datetime.now()
        naver_df["updated_at"] = datetime.now()
        today_transform_naver_complex_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + NAVER_COMPLEX_FILE_NAME)
        upload_to_s3(today_transform_naver_complex_file_name, naver_df)

    @task
    def load_to_redshift_new_naver_complex():
        today_transform_naver_complex_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + NAVER_COMPLEX_FILE_NAME)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {SCHEMA}.complex
                    FROM 's3://team-ariel-2-data/data/{today_transform_naver_complex_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV 
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {SCHEMA}.complex')

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def fetch_new_dabang_complex():
        new_dabang_complex_df = get_new_dabang_complex()
        if new_dabang_complex_df.empty:
            return False

        today_dabang_complex_file_name = get_today_file_name(PREFIX_INTEGRATE + DABANG_COMPLEX_FILE_NAME)
        upload_to_s3(today_dabang_complex_file_name, new_dabang_complex_df)
        return True

    @task
    def transform_new_dabang_complex():
        today_dabang_complex_file_name = get_today_file_name(PREFIX_INTEGRATE + DABANG_COMPLEX_FILE_NAME)
        dabang_df = get_df_from_s3_csv(today_dabang_complex_file_name)
        dabang_df["approve_date"] = pd.to_datetime(dabang_df["approve_date"].astype(str), errors="coerce",
                                                   format="%Y.%m.%d")
        dabang_df["created_at"] = datetime.now()
        dabang_df["updated_at"] = datetime.now()
        today_transform_dabang_complex_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + DABANG_COMPLEX_FILE_NAME)
        upload_to_s3(today_transform_dabang_complex_file_name, dabang_df)

    @task
    def load_to_redshift_new_dabang_complex():
        today_transform_dabang_complex_file_name = get_today_file_name(
            PREFIX_TRANSFORM + PREFIX_INTEGRATE + DABANG_COMPLEX_FILE_NAME)
        cur = get_redshift_connection()
        cur.execute(f"""
                    COPY {SCHEMA}.complex
                    FROM 's3://team-ariel-2-data/data/{today_transform_dabang_complex_file_name}'
                    IAM_ROLE '{iam_role}'
                    CSV 
                    IGNOREHEADER 1;""")
        cur.close()
        logging.info(f'Data successfully loaded into {SCHEMA}.complex')

    @task(trigger_rule="none_failed")
    def fetch_vworld_coordinate_to_address():
        real_estate_df = get_filtered_region_all_complex()
        batch_records = []
        for idx, row in real_estate_df.iterrows():
            _json = get_coordinate_convert_address(row["latitude"], row["longitude"])
            if _json:
                region_gu = _json.get("region_gu")
                region_dong = _json.get("region_dong")
                cortar_no = _json.get("cortar_no")
                batch_records.append((region_gu, region_dong, cortar_no, row["id"]))

            if len(batch_records) >= BATCH_SIZE:
                update_real_estate_batch_region(batch_records)
                batch_records.clear()

        if batch_records:
            update_real_estate_batch_region(batch_records)

    load_to_redshift_naver = load_to_redshift_new_naver_complex()
    load_to_redshift_dabang = load_to_redshift_new_dabang_complex()
    fetch_vworld = fetch_vworld_coordinate_to_address()
    fetch_new_naver_complex() >> transform_new_naver_complex() >> load_to_redshift_naver >> fetch_vworld
    fetch_new_dabang_complex() >> transform_new_dabang_complex() >> load_to_redshift_dabang >> fetch_vworld



integrate_complex()
