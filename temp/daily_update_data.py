import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

today = (datetime.now()+timedelta(hours=9)).strftime(r"%Y%m%d")
querydate = today[:6]

def data_extract(url, key, dates, cols, save_fname, make_list):
    region = [11110, 11140, 11170, 11200, 11215, 11230, 11260, 11290, 11305, 11320, 11350, 11380, 11410, 11440, 11470, 11500, 11530, 11545,
              11560, 11590, 11620 ,11650, 11680, 11710, 11740]
    data = []
    for i in region:
        for j in dates:
            params ={'serviceKey' : f'{key}', 
                    'LAWD_CD' : f'{i}', 
                'DEAL_YMD' : f'{j}'}
            response = requests.get(url, params=params).content
            soup = BeautifulSoup(response, 'lxml-xml')
            rows = soup.find_all('item')
            make_list(data, rows, cols)

    df = pd.DataFrame(data, columns=cols, dtype = object) 
    df.to_csv(f'{save_fname}.csv') 

# 아파트매매
url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
key = open('key.txt', 'r')
cols = ['년','월', '일', '법정동', '아파트',  '전용면적', '거래금액', '건축년도',
        '지번', '지역코드', '층', '해제여부', '해제사유발생일', '거래유형', '중개사소재지', '등기일자', '매도자', '매수자','동']
def apartment_sell(data, source, cols):
    for row in source:
        try:
            year = row.find(cols[0]).get_text()
        except: year = " "
        try:
            month = row.find(cols[1]).get_text()
        except: month = " "
        try:
            day = row.find(cols[2]).get_text()
        except: day
        try:
            dong = row.find(cols[3]).get_text()
        except: dong = " "
        try:
            name = row.find(cols[4]).get_text()
        except: name = " "
        try:
            area = row.find(cols[5]).get_text()
        except: area = " "
        try:
            price = row.find(cols[6]).get_text()
        except: price = " "
        try:
            buildyear = row.find(cols[7]).get_text()
        except: buildyear = " "
        try:
            addr = row.find(cols[8]).get_text()
        except: addr = " "
        try:
            code = row.find(cols[9]).get_text()
        except: code = " "
        try:
            floor = row.find(cols[10]).get_text()
        except: floor = " "
        try:
            yn = row.find(cols[11]).get_text()
        except: yn = " "
        try:
            yndate = row.find(cols[12]).get_text()
        except: yndate = " "
        try:
            type = row.find(cols[13]).get_text()
        except: type = " "
        try:
            realtor = row.find(cols[14]).get_text()
        except: realtor = " "
        try:
            contract_date = row.find(cols[15]).get_text()
        except: contract_date = " "
        try:
            seller = row.find(cols[16]).get_text()
        except: sellor = " "
        try:
            buyer = row.find(cols[17]).get_text()
        except: buyer = " "
        try:
            buildingnum = row.find(cols[18]).get_text()
        except: buildingnum = " "
        data.append([year, month, day, dong, name, area, price, buildyear, addr, code, floor, yn, yndate, type, realtor, contract_date, seller, buyer, buildingnum])
data_extract(url, key, querydate, cols, "아파트매매", apartment_sell)

# 아파트전월세
url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent"
cols = ['년', '월', '일', '법정동', '아파트',  '전용면적', '갱신요구권사용', '건축년도', '지번', '계약구분', '계약기간', '보증금액', 
        '월세금액','종전계약보증금', '종전계약월세', '지역코드', '층']
def apartment_rent(data, source, cols):
    for row in source:
        try:
            year = row.find(cols[0]).get_text()
        except: year = " "
        try:
            month = row.find(cols[1]).get_text()
        except: month = " "
        try:
            day = row.find(cols[2]).get_text()
        except: day
        try:
            dong = row.find(cols[3]).get_text()
        except: dong = " "
        try:
            name = row.find(cols[4]).get_text()
        except: name = " "
        try:
            area = row.find(cols[5]).get_text()
        except: area = " "
        try:
            uses = row.find(cols[6]).get_text()
        except: uses = " "
        try:
            buildyear = row.find(cols[7]).get_text()
        except: buildyear = " "
        try:
            addr = row.find(cols[8]).get_text()
        except: addr = " "
        try:
            type = row.find(cols[9]).get_text()
        except: type = " "
        try:
            length = row.find(cols[10]).get_text()
        except: length = " "
        try:
            deposit = row.find(cols[11]).get_text()
        except: deposit = " "
        try:
            rent = row.find(cols[12]).get_text()
        except: rent = " "
        try:
            before_deposit = row.find(cols[13]).get_text()
        except: before_deposit = " "
        try:
            before_rent = row.find(cols[14]).get_text()
        except: before_rent = " "
        try:
            code = row.find(cols[15]).get_text()
        except: code = " "
        try:
            floor = row.find(cols[16]).get_text()
        except: floor = " "
        data.append([year, month, day, dong, name, area, uses, buildyear, addr, type, length, deposit, rent, before_deposit, before_rent, code, floor])
data_extract(url, key, querydate, cols, "아파트전월세", apartment_rent)

# 연립다세대 전월세
url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent'
cols = ['년', '월', '일', '법정동', '연립다세대',  '전용면적', '갱신요구권사용', '건축년도', '지번', '계약구분', '계약기간', '보증금액', 
        '월세금액','종전계약보증금', '종전계약월세', '지역코드', '층']
def billa_rent(data, source, cols):
    for row in source:
        try:
            year = row.find(cols[0]).get_text()
        except: year = " "
        try:
            month = row.find(cols[1]).get_text()
        except: month = " "
        try:
            day = row.find(cols[2]).get_text()
        except: day
        try:
            dong = row.find(cols[3]).get_text()
        except: dong = " "
        try:
            name = row.find(cols[4]).get_text()
        except: name = " "
        try:
            area = row.find(cols[5]).get_text()
        except: area = " "
        try:
            uses = row.find(cols[6]).get_text()
        except: uses = " "
        try:
            buildyear = row.find(cols[7]).get_text()
        except: buildyear = " "
        try:
            addr = row.find(cols[8]).get_text()
        except: addr = " "
        try:
            type = row.find(cols[9]).get_text()
        except: type = " "
        try:
            length = row.find(cols[10]).get_text()
        except: length = " "
        try:
            deposit = row.find(cols[11]).get_text()
        except: deposit = " "
        try:
            rent = row.find(cols[12]).get_text()
        except: rent = " "
        try:
            before_deposit = row.find(cols[13]).get_text()
        except: before_deposit = " "
        try:
            before_rent = row.find(cols[14]).get_text()
        except: before_rent = " "
        try:
            code = row.find(cols[15]).get_text()
        except: code = " "
        try:
            floor = row.find(cols[16]).get_text()
        except: floor = " "
        data.append([year, month, day, dong, name, area, uses, buildyear, addr, type, length, deposit, rent, before_deposit, before_rent, code, floor])
data_extract(url, key, querydate, cols, "연립다세대전월세", billa_rent)

# 연립다세대 매매
url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade'
cols = ['년','월', '일', '법정동', '연립다세대',  '전용면적', '거래금액', '건축년도',
        '지번', '지역코드', '층', '해제여부', '해제사유발생일', '거래유형', '중개사소재지', '등기일자', '매도자', '매수자','대지권면적']
def billa_sell(data, source, cols):
    for row in source:
        try:
            year = row.find(cols[0]).get_text()
        except: year = " "
        try:
            month = row.find(cols[1]).get_text()
        except: month = " "
        try:
            day = row.find(cols[2]).get_text()
        except: day
        try:
            dong = row.find(cols[3]).get_text()
        except: dong = " "
        try:
            name = row.find(cols[4]).get_text()
        except: name = " "
        try:
            area = row.find(cols[5]).get_text()
        except: area = " "
        try:
            price = row.find(cols[6]).get_text()
        except: price = " "
        try:
            buildyear = row.find(cols[7]).get_text()
        except: buildyear = " "
        try:
            addr = row.find(cols[8]).get_text()
        except: addr = " "
        try:
            code = row.find(cols[9]).get_text()
        except: code = " "
        try:
            floor = row.find(cols[10]).get_text()
        except: floor = " "
        try:
            yn = row.find(cols[11]).get_text()
        except: yn = " "
        try:
            yndate = row.find(cols[12]).get_text()
        except: yndate = " "
        try:
            type = row.find(cols[13]).get_text()
        except: type = " "
        try:
            realtor = row.find(cols[14]).get_text()
        except: realtor = " "
        try:
            contract_date = row.find(cols[15]).get_text()
        except: contract_date = " "
        try:
            seller = row.find(cols[16]).get_text()
        except: sellor = " "
        try:
            buyer = row.find(cols[17]).get_text()
        except: buyer = " "
        try:
            buildingnum = row.find(cols[18]).get_text()
        except: buildingnum = " "
        data.append([year, month, day, dong, name, area, price, buildyear, addr, code, floor, yn, yndate, type, realtor, contract_date, seller, buyer, buildingnum])
data_extract(url, key, querydate, cols, "연립다세대매매", billa_sell)