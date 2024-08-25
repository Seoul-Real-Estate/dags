import logging
import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(file_name, data_frame, bucket_name="team-ariel-2-data", prefix_key="data/"):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        csv_buffer = StringIO()
        key = prefix_key + file_name
        data_frame.to_csv(csv_buffer, index=False)
        s3_hook.load_string(string_data=csv_buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True)
        logging.info(f"Upload to S3 {bucket_name}: {key}")
    except Exception as e:
        logging.error(f"Error Upload to S3 {bucket_name}: {e}")
        raise


def exists_s3_file(file_name, bucket_name="team-ariel-2-data", prefix_key="data/"):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        key = prefix_key + file_name
        return s3_hook.check_for_key(key=key, bucket_name=bucket_name)
    except Exception as e:
        logging.error(f"Error AWS S3: {e}")
        raise


def get_df_from_s3_csv(file_name, bucket_name="team-ariel-2-data", prefix_key="data/"):
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
        key = prefix_key + file_name
        csv_data = s3_hook.get_key(key=key, bucket_name=bucket_name).get()['Body']
        return pd.read_csv(csv_data)
    except FileNotFoundError as fe:
        logging.error(
            f"FileNotFoundError: The file '{file_name}' could not be found in the S3 bucket '{bucket_name}'. "
            f"Check the S3 permissions as well. Detailed error: {fe}"
        )
        raise
    except Exception as e:
        logging.error(
            f"Unexpected error: An unexpected error occurred while attempting to read the CSV file from S3 at path '{key}'. "
            f"Detailed error: {e}"
        )
        raise
