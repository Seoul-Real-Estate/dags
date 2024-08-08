from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2
import plugins.seoul_realestate as realestate
from io import StringIO
import pandas as pd
import numpy as np

from datetime import datetime
from datetime import timedelta

import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='final_redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_apartment_rent(reclass, url, key, origincols, columns):
    raw_df = reclass.data_extract(url, key, origincols, columns, reclass.apartment_rent_list)
    return raw_df

@task
def apartment_rent_preproc(reclass, raw_df, columns):
    preproc_df = reclass.apartment_rent_preprocessing(raw_df, columns)
    return preproc_df

@task
def apartment_rent_uploadS3(reclass, preproc_df):
    csv_buffer = StringIO()
    preproc_df.to_csv(csv_buffer, index=False)
    
    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f'data/apartment_rent_{reclass.trademonth}_{reclass.rundate}.csv',
        bucket_name='team-ariel-2-data',
        replace=True
    )

def check_date(today):
    if today[-2:] == '01':
        return "apartment_rent_load_1st"
    else:
        return "apartment_rent_compare"
    
@task
def apartment_rent_load_1st(reclass, preproc_df, columns, schema, table, upload_fname, credentials):
    logging.info("apartment rent load 1st started")
    new_df = reclass.compare_add_latlon(pd.DataFrame([], columns=preproc_df.columns), preproc_df, columns)
    csv_buffer = StringIO()
    new_df.to_csv(csv_buffer, index=False)

    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f'data/{upload_fname}',
        bucket_name='team-ariel-2-data',
        replace=True
    )

    logging.info("apartment rent load to redshift")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        sql = f"""
            COPY {schema}.{table}
            FROM 's3://team-ariel-2-data/data/{upload_fname}'
            credentials '{credentials}'
            delimiter ',' dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 removequotes MAXERROR 10;
            """
        cur.execute(sql)
        cur.execute("COMMIT;")
        logging.info("load done")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.info("ROLLBACK")
        cur.execute("ROLLBACK;")

@task
def apartment_rent_compare(reclass, before_fname, now_fname, columns):
    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    bucket_name = 'team-ariel-2-data'

    before_key = f'data/{before_fname}'
    bfile_content = s3_hook.read_key(before_key, bucket_name)
    bcsv_buffer = StringIO(bfile_content)
    before_df = pd.read_csv(bcsv_buffer, header=0)

    now_key = f'data/{now_fname}'
    file_content = s3_hook.read_key(now_key, bucket_name)
    csv_buffer = StringIO(file_content)
    now_df = pd.read_csv(csv_buffer, header=0)

    new_df = reclass.compare_add_latlon(before_df, now_df, columns)
    return new_df


def check_new(new_df):
    if len(new_df) > 0:
        return "apartment_rent_new_uploadS3"
    else:
        return "end_task"

@task
def apartment_rent_new_uploadS3(new_df, upload_fname):
    csv_buffer = StringIO()
    new_df.to_csv(csv_buffer, index=False)

    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f'data/{upload_fname}',
        bucket_name='team-ariel-2-data',
        replace=True
    )


@task
def apartment_rent_load(schema, table, upload_fname, credentials):
    logging.info("apartment rent load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        sql = f"""
            COPY {schema}.{table}
            FROM 's3://team-ariel-2-data/data/{upload_fname}'
            credentials '{credentials}'
            delimiter ',' dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 removequotes MAXERROR 10;
            """
        cur.execute(sql)
        cur.execute("COMMIT;")
        logging.info("load done")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.info("ROLLBACK")
        cur.execute("ROLLBACK;")

with DAG(
    dag_id='rent_apartment',
    start_date=datetime(2024, 7, 24),
    schedule_interval='50 9 * * *',
    max_active_runs=1,
    catchup=False, 
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    schema = 'raw_data'
    table = 'rent'  
    key = Variable.get('gov_data_api_key')
    today = datetime.utcnow().strftime("%Y%m%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")

    url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
    credential = Variable.get('S3_redshift_credentail_key')

    apartment_rent_origin_cols = ['dealYear','dealMonth', 'dealDay', 'umdNm', 'aptNm',  'excluUseAr', 'useRRRight', 'buildYear', 'jibun', 'contractType', 
                            'contractTerm', 'deposit', 'monthlyRent', 'preDeposit', 'preMonthlyRent', 'sggCd', 'floor']
    apartment_rent_cols = ['year', 'month', 'day', '법정동명', '건물명', '임대면적', '계약갱신권사용여부', '건축년도', '지번', '신규갱신여부',
                        '계약기간', '보증금', '임대료', '종전보증금', '종전임대료', '지역코드', '층']
    preproc_cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '전월세구분', '임대면적', '보증금', '임대료',
                    '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
    fin_cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세구분', '임대면적', '보증금', '임대료',
                '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']

    sre = realestate.realestate(today, today[:6])
    before_fname = f"apartment_rent_{today[:6]}_{yesterday}.csv"
    now_fname = f"apartment_rent_{today[:6]}_{today}.csv"
    upload_fname = f"apartment_rent_upload_{today[:6]}_{today}.csv"

    raw_df = get_apartment_rent(sre, url, key, apartment_rent_origin_cols, apartment_rent_cols)
    preproc_df = apartment_rent_preproc(sre, raw_df, preproc_cols)
    upload_task = apartment_rent_uploadS3(sre, preproc_df) 
    check_date_task = BranchPythonOperator(
        task_id = "check_date",
        python_callable=check_date,
        op_args=[today],
    )
    load_1st_task = apartment_rent_load_1st(sre, preproc_df, fin_cols, schema, table, upload_fname, credential)
    new_df = apartment_rent_compare(sre, before_fname, now_fname, fin_cols)
    check_new_task = BranchPythonOperator(
        task_id = "check_new",
        python_callable=check_new,
        op_args=[new_df],
    )
    end_task = EmptyOperator(task_id="end_task")
    load_task = apartment_rent_new_uploadS3(new_df, upload_fname)
    load_redshif_task = apartment_rent_load(schema, table, upload_fname, credential)

    raw_df >> preproc_df >> upload_task >> check_date_task
    check_date_task >> [load_1st_task, new_df]
    new_df >> check_new_task
    check_new_task >> [load_task, end_task]
    load_task >> load_redshif_task
