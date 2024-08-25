import logging
from typing import List
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


def get_redshift_connection(autocommit=True):
    try:
        hook = RedshiftSQLHook(redshift_conn_id="redshift_dev")
        return hook.get_conn()
    except Exception as e:
        logging.error(f"Error connection to AWS Redshift DB: {e}")
        raise


def get_primary_keys(schema: str, table: str, column: str) -> List[str]:
    query = f"SELECT {column} FROM {schema}.{table}"
    conn = None
    cur = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return [row[0] for row in rows]
    except Exception as e:
        logging.error(f"Error Failed to fetch primary keys from {schema}.{table}: {e}")
        return []
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def load_to_redshift_from_s3_csv(schema: str, table: str, s3_path: str):
    iam_role = Variable.get("aws_iam_role")
    conn = None
    cur = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        cur.execute(f"""
            COPY {schema}.{table}
            FROM {s3_path}
            IAM_ROLE "{iam_role}"
            CSV
            IGNOREHEADER 1;""")
        logging.info(f'SUCCESS AWS Redshift loaded into {schema}.{table}')
    except Exception as e:
        logging.error(f"Error Failed to COPY {schema}.{table} from {s3_path}: {e}")
        raise
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
