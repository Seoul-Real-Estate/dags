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
def get_officetel_trade(reclass, url, key, origincols, columns):
    raw_df = reclass.data_extract(url, key, origincols, columns, reclass.officetel_trade_list)
    return raw_df

@task
def officetel_trade_preproc(reclass, raw_df, columns):
    preproc_df = reclass.officetel_trade_preprocessing(raw_df, columns)
    return preproc_df

@task
def officetel_trade_uploadS3(reclass, preproc_df):
    csv_buffer = StringIO()
    preproc_df.to_csv(csv_buffer, index=False)
    
    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f'data/officetel_trade_{reclass.trademonth}_{reclass.rundate}.csv',
        bucket_name='team-ariel-2-data',
        replace=True
    )

def check_date(today):
    if today[-2:] == '01':
        return "officetel_trade_load_1st"
    else:
        return "officetel_trade_compare"
    
@task
def officetel_trade_load_1st(reclass, preproc_df, columns, schema, table, upload_fname, credentials):
    logging.info("officetel trade load 1st started")
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

    logging.info("officetel trade load to redshift")
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
def officetel_trade_compare(reclass, before_fname, now_fname, columns):
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

    before_df['건축년도'] = before_df['건축년도'].replace(' ', np.nan).astype(float).fillna(0).astype(int)
    now_df['건축년도'] = now_df['건축년도'].replace(' ', np.nan).astype(float).fillna(0).astype(int)
    
    new_df = reclass.compare_add_latlon(before_df, now_df, columns)
    return new_df


def check_new(new_df):
    if len(new_df) > 0:
        return "officetel_trade_new_uploadS3"
    else:
        return "end_task"

@task
def officetel_trade_new_uploadS3(new_df, upload_fname):
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
def officetel_trade_load(schema, table, upload_fname, credentials):
    logging.info("officetel trade load started")
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
    dag_id='trade_officetel',
    start_date=datetime(2024, 7, 24),
    schedule_interval='35 11 * * *',
    max_active_runs=1,
    catchup=False, 
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    schema = 'raw_data'
    table = 'trade'  
    key = Variable.get('gov_data_api_key')
    today = datetime.utcnow().strftime("%Y%m%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")

    url = 'http://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade'
    credential = Variable.get('S3_redshift_credentail_key')

    officetel_trade_origin_cols = ['dealYear','dealMonth', 'dealDay', 'umdNm', 'offiNm', 'excluUseAr', 'dealAmount', 'buildYear',
                            'jibun', 'sggCd', 'floor', 'cdealDay', 'dealingGbn']
    officetel_trade_cols = ['year', 'month', 'day', '법정동명', '건물명', '전용면적', '물건금액', '건축년도', '지번', '지역코드', '층', '취소일', '신고구분']
    preproc_cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '물건금액', '전용면적', '토지면적', '권리구분',
                '취소일', '건축년도', '건물용도', '신고구분', '등기일자']
    fin_cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액', '전용면적', '토지면적', '권리구분',
                '취소일', '건축년도', '건물용도', '신고구분', '등기일자']

    sre = realestate.realestate(today, today[:6])
    before_fname = f"officetel_trade_{today[:6]}_{yesterday}.csv"
    now_fname = f"officetel_trade_{today[:6]}_{today}.csv"
    upload_fname = f"officetel_trade_upload_{today[:6]}_{today}.csv"

    raw_df = get_officetel_trade(sre, url, key, officetel_trade_origin_cols, officetel_trade_cols)
    preproc_df = officetel_trade_preproc(sre, raw_df, preproc_cols)
    upload_task = officetel_trade_uploadS3(sre, preproc_df) 
    check_date_task = BranchPythonOperator(
        task_id = "check_date",
        python_callable=check_date,
        op_args=[today],
    )
    load_1st_task = officetel_trade_load_1st(sre, preproc_df, fin_cols, schema, table, upload_fname, credential)
    new_df = officetel_trade_compare(sre, before_fname, now_fname, fin_cols)
    check_new_task = BranchPythonOperator(
        task_id = "check_new",
        python_callable=check_new,
        op_args=[new_df],
    )
    end_task = EmptyOperator(task_id="end_task")
    load_task = officetel_trade_new_uploadS3(new_df, upload_fname)
    load_redshif_task = officetel_trade_load(schema, table, upload_fname, credential)

    raw_df >> preproc_df >> upload_task >> check_date_task
    check_date_task >> [load_1st_task, new_df]
    new_df >> check_new_task
    check_new_task >> [load_task, end_task]
    load_task >> load_redshif_task