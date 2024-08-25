import logging
import requests
from requests import JSONDecodeError, HTTPError, RequestException


def get(url, params=None, headers=None):
    try:
        res = requests.get(url, params=params, headers=headers)
        res.raise_for_status()
        return res
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        raise
    except RequestException as re:
        logging.error(f"Request error occurred: {re}")
        raise
    except Exception as e:
        logging.error(f"Other error occurred: {e}")
        raise


def get_json(url, params=None, headers=None):
    try:
        res = get(url, params=params, headers=headers)
        return res.json()
    except JSONDecodeError as je:
        logging.error(f"JSON decoding failed: {je}")
        raise
    except Exception as e:
        logging.error(f"Other error occurred: {e}")
        raise
