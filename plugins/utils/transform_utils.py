import logging
import pandas as pd
from datetime import date


def get_today_file_name(file_name):
    today = date.today()
    return f'{today}_{file_name}'


def clean_numeric_column(df, column_name):
    try:
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
    except Exception as e:
        logging.error(f"Error converting columns '{column_name}' to numeric: {e}")
