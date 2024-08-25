import pandas as pd
from airflow.models import Variable
from plugins.utils import requests_utils

REGION_URL = "https://new.land.naver.com/api/regions/list?cortarNo="
APT_COMPLEX_DETAIL_URL = "https://new.land.naver.com/api/complexes/"
APT_COMPLEX_DETAIL_PARAMS = {"sameAddressGroup": "false"}
APT_REAL_ESTATE_URL = "https://new.land.naver.com/api/articles/complex/"
APT_REAL_ESTATE_DETAIL_URL = "https://new.land.naver.com/api/articles/"
APT_REAL_ESTATE_DETAIL_PARAMS = {"complexNo": ""}
VILLA_URL = "https://new.land.naver.com/api/articles"
VILLA_DETAIL_PARAMS = {"complexNo": ""}
BASE_HEADERS = {
    "Accept-Encoding": "gzip",
    "Host": "new.land.naver.com",
    "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}


def get_region_info(cortarNo):
    url = f"{REGION_URL}{cortarNo}"
    _json = requests_utils.get_json(url, headers=BASE_HEADERS)
    return pd.DataFrame(_json["regionList"])


def get_apt_complex(cortarNo):
    complex_url = f"https://new.land.naver.com/api/regions/complexes?cortarNo={cortarNo}&realEstateType=OPST:APT&order="
    _json = requests_utils.get_json(complex_url, headers=BASE_HEADERS)
    return pd.DataFrame(_json["complexList"])


def get_apt_complex_detail(complexNo):
    url = APT_COMPLEX_DETAIL_URL + complexNo
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get("naver_token")
    headers.update({"Authorization": f"{naver_token}"})
    _json = requests_utils.get_json(url, params=APT_COMPLEX_DETAIL_PARAMS, headers=headers)
    return _json["complexDetail"]


def get_apt_real_estate(complexNo):
    url = APT_REAL_ESTATE_URL + complexNo
    params = {
        "realEstateType": "APT",
        "tag": "%3A%3A%3A%3A%3A%3A%3A%3",
        "rentPriceMin": 0,
        "rentPriceMax": 900000000,
        "priceMin": 0,
        "priceMax": 900000000,
        "areaMin": 0,
        "areaMax": 900000000,
        "showArticle": "false",
        "sameAddressGroup": "false",
        "priceType": "RETAIL",
        "page": 1,
        "complexNo": int(complexNo),
        "type": "list",
        "order": "rank",
        "tradeType": "",
        "oldBuildYears": "",
        "recentlyBuildYears": "",
        "minHouseHoldCount": "",
        "maxHouseHoldCount": "",
        "minMaintenanceCost": "",
        "maxMaintenanceCost": "",
        "directions": "",
        "buildingNos": "",
        "areaNos": ""
    }
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get("naver_token")
    headers.update({"Authorization": f"{naver_token}"})
    _json = requests_utils.get_json(url, params=params, headers=headers)
    return pd.DataFrame(_json["articleList"])


def get_real_estate_detail(articleNo):
    url = APT_REAL_ESTATE_DETAIL_URL + articleNo
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get('naver_token')
    headers.update({"Authorization": f"{naver_token}"})
    return requests_utils.get_json(url, params=APT_REAL_ESTATE_DETAIL_PARAMS, headers=headers)


def get_villa_real_estate(cortarNo):
    params = {
        "cortarNo": int(cortarNo),
        "order": "rank",
        "realEstateType": "JWJT:HOJT:VL:DDDGG:SGJT",
        "tradeType": "",
        "tag": " ::::::::",
        "rentPriceMin": " 0",
        "rentPriceMax": " 900000000",
        "priceMin": " 0",
        "priceMax": " 900000000",
        "areaMin": " 0",
        "areaMax": " 900000000",
        "oldBuildYears": "",
        "recentlyBuildYears": "",
        "minHouseHoldCount": "",
        "maxHouseHoldCount": "",
        "showArticle": " false",
        "sameAddressGroup": " false",
        "minMaintenanceCost": "",
        "maxMaintenanceCost": "",
        "priceType": " RETAIL",
        "directions": "",
        "page": " 1",
        "articleState": ""
    }
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get("naver_token")
    headers.update({"Authorization": f"{naver_token}"})
    _json = requests_utils.get_json(VILLA_URL, params=params, headers=headers)
    return pd.DataFrame(_json["articleList"])


def get_villa_real_estate_detail(articleNo):
    url = VILLA_URL + f"/{articleNo}"
    headers = BASE_HEADERS.copy()
    naver_token = Variable.get("naver_token")
    headers.update({"Authorization": f"{naver_token}"})
    return requests_utils.get_json(url, params=VILLA_DETAIL_PARAMS, headers=headers)
