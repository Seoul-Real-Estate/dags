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
def get_house_rent(reclass, url, key, origincols, columns):
    raw_df = reclass.data_extract(url, key, origincols, columns, reclass.house_rent_list)
    return raw_df

@task
def house_rent_preproc(reclass, raw_df, columns):
    preproc_df = reclass.house_rent_preprocessing(raw_df, columns)
    return preproc_df

@task
def house_rent_uploadS3(reclass, preproc_df):
    csv_buffer = StringIO()
    preproc_df.to_csv(csv_buffer, index=False)
    
    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f'data/house_rent_{reclass.trademonth}_{reclass.rundate}.csv',
        bucket_name='team-ariel-2-data',
        replace=True
    )

@task
def house_rent_compare(reclass, before_fname, now_fname, columns):
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
        return "house_rent_new_uploadS3"
    else:
        return "end_task2"

@task
def house_rent_new_uploadS3(new_df, upload_fname):
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
def house_rent_load(schema, table, upload_fname, credentials):
    logging.info("house rent load started")
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
    dag_id='rent_house_2mon',
    start_date=datetime(2024, 7, 24),
    schedule_interval='30 10 * * *',
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
    if int(today[4:6]) > 2:
        mon2 = str(int(today[:6])-2)
    elif int(today[4:6]) > 1:
        mon2 = str(int(today[:4])-1)+'12'
    else:
        mon2 = str(int(today[:4])-1)+'11'

    url = 'http://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent'
    credential = Variable.get('S3_redshift_credentail_key')

    house_rent_origin_cols = ['dealYear','dealMonth', 'dealDay', 'umdNm', 'totalFloorAr', 'useRRRight', 'buildYear', 'contractType', 
                    'contractTerm', 'deposit', 'monthlyRent', 'preDeposit', 'preMonthlyRent', 'sggCd']
    house_rent_cols = ['year', 'month', 'day', '법정동명', '임대면적', '계약갱신권사용여부', '건축년도', '신규갱신여부',
                        '계약기간', '보증금', '임대료', '종전보증금', '종전임대료', '지역코드']
    preproc_cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '전월세구분', '임대면적', '보증금', '임대료',
                    '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
    fin_cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세구분', '임대면적', '보증금', '임대료',
                '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']

    sre2 = realestate.realestate(today, mon2)
    before_fname2 = f"house_rent_{mon2}_{yesterday}.csv"
    now_fname2 = f"house_rent_{mon2}_{today}.csv"
    upload_fname2 = f"house_rent_upload_{mon2}_{today}.csv"

    raw_df2 = get_house_rent(sre2, url, key, house_rent_origin_cols, house_rent_cols)
    preproc_df2 = house_rent_preproc(sre2, raw_df2, preproc_cols)
    upload_task2 = house_rent_uploadS3(sre2, preproc_df2) 
    new_df2 = house_rent_compare(sre2, before_fname2, now_fname2, fin_cols)
    check_new2 = BranchPythonOperator(
        task_id = "check_new2",
        python_callable=check_new,
        op_args=[new_df2],
    )
    end_task2 = EmptyOperator(task_id="end_task2")
    load_task2 = house_rent_new_uploadS3(new_df2, upload_fname2)
    load_redshif_task2 = house_rent_load(schema, table, upload_fname2, credential)

    raw_df2 >> preproc_df2 >> upload_task2 >> new_df2 >> check_new2
    check_new2 >> [load_task2, end_task2]
    load_task2 >> load_redshif_task2
