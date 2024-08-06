import pandas as pd
from geopy.geocoders import Nominatim
import numpy as np
import time
import requests

"""
, dtype={
    '접수연도' : 'int64',
    '자치구코드' : 'string',
    '자치구명' : 'string',
    '법정동코드' : 'int64',
    '법정동명' : 'string',
    '지번구분' : 'int64',
    '지번구분명' : 'string',
    '본번' : 'int64',
    '부번' : 'int64',
    '건물명' : 'string',
    '계약일' : 'int64',
    '물건금액(만원)' : 'int64',
    '건물면적(㎡)' : 'float',
    '토지면적(㎡)' : 'float',
    '층' : 'int64',
    '권리구분' : 'string',
    '취소일' : 'int64',
    '건축년도' : 'int64',
    '건물용도' : 'string',
    '신고구분' : 'string',
    '신고한 개업공인중개사 시군구명' : 'string',
}
"""

"""
# USE geopy
def get_lat_lng(add):
    geolocator = Nominatim(user_agent='South Korea')  # Fix the typo here
    location = geolocator.geocode(add)

    if location:
        latitude = location.latitude
        longitude = location.longitude
        return latitude, longitude
    else:
        print('지번 주소 문제')
        print(add)
        return None, None

trade = pd.read_csv('data/seoul_realestate.csv', header = 0, skipinitialspace=True)
trade = trade.drop(trade[trade['자치구코드'] > 20000].index)
trade = trade.drop(['접수연도', '신고한 개업공인중개사 시군구명', '자치구코드', '법정동코드', '지번구분', '지번구분명'], axis=1)
trade = trade.fillna(-9999)
tt = time.time()
newdata = []
for idx, row in trade.iterrows():
    if row['본번'] != -9999 and row['부번'] != 0:
        row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"])) + "-" + str(int(row["부번"]))
        lat, lon = get_lat_lng(row['주소'])
        row['위도'] = lat
        row['경도'] = lon
    elif row['본번'] != -9999 and row['부번'] == 0:
        row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"]))
        lat, lon = get_lat_lng(row['주소'])
        row['위도'] = lat
        row['경도'] = lon
    else:
        row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"]
        row['위도'] = -9999
        row['경도'] = -9999
    if idx == 10:
        print(idx, time.time()-tt)
        break
    newdata.append(row)

df = pd.DataFrame(newdata, dtype = object) 
df = df.replace({-9999: None})
df.to_csv(f'test.csv') 
"""

# USE api
# def get_lat_lng(add):
#     geolocator = Nominatim(user_agent='South Korea')  # Fix the typo here
#     location = geolocator.geocode(add)

#     if location:
#         latitude = location.latitude
#         longitude = location.longitude
#         return latitude, longitude
#     else:
#         print('지번 주소 문제')
#         print(add)
#         return None, None

# trade = pd.read_csv('data/seoul_realestate.csv', header = 0, skipinitialspace=True)
# trade = trade.drop(trade[trade['자치구코드'] > 20000].index)
# trade = trade.drop(['접수연도', '신고한 개업공인중개사 시군구명', '자치구코드', '법정동코드', '지번구분', '지번구분명'], axis=1)
# trade = trade.fillna(-9999)
# trade = trade[~trade.duplicated(subset=['법정동명', '본번', '부번'])]
# tt = time.time()
# apiurl = "https://api.vworld.kr/req/address?"
# newdata = []
# count = 0
# for idx, row in trade.iterrows():
#     if idx < 88426:
#         continue
#     if row['본번'] != -9999 and row['부번'] != 0:
#         row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"])) + "-" + str(int(row["부번"]))
#         params = {
#             "service": "address",
#             "request": "getcoord",
#             "crs": "epsg:4326",
#             f"address": f"{row['주소']}",
#             "format": "json",
#             "type": "parcel",
#             "key": "28010AC2-7187-3642-BC8C-816A34DDF7CC"
#         }
#         try:
#             response = requests.get(apiurl, params=params)
#         except Exception as e:
#             print(e)
#             break
#         count += 1
#         if response.status_code == 200:
#             try:
#                 lat = response.json()["response"]["result"]['point']['x']
#                 lon = response.json()["response"]["result"]['point']['y']
#                 row['위도'] = lat
#                 row['경도'] = lon
#             except KeyError:
#                 print(response.json())
#                 row['위도'] = -9999
#                 row['경도'] = -9999
#         else:
#             row['위도'] = -9999
#             row['경도'] = -9999
#     elif row['본번'] != -9999 and row['부번'] == 0:
#         row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"]))
#         params = {
#             "service": "address",
#             "request": "getcoord",
#             "crs": "epsg:4326",
#             f"address": f"{row['주소']}",
#             "format": "json",
#             "type": "parcel",
#             "key": "28010AC2-7187-3642-BC8C-816A34DDF7CC"
#         }
#         try:
#             response = requests.get(apiurl, params=params)
#         except Exception as e:
#             print(e)
#             break
#         count += 1
#         if response.status_code == 200:
#             try:
#                 lat = response.json()["response"]["result"]['point']['x']
#                 lon = response.json()["response"]["result"]['point']['y']
#                 row['위도'] = lat
#                 row['경도'] = lon
#             except KeyError:
#                 print(response.json())
#                 row['위도'] = -9999
#                 row['경도'] = -9999
#         else:
#             row['위도'] = -9999
#             row['경도'] = -9999
#     else:
#         row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"]
#         row['위도'] = -9999
#         row['경도'] = -9999
#     if idx % 100 == 0:
#         print(idx, time.time()-tt)
#         print(count)
#     if count == 35000:
#         print(count)
#         break
#     newdata.append(row)

# df = pd.DataFrame(newdata, dtype = object) 
# df = df.replace({-9999: None})
# df.to_csv(f'temp_trade3.csv') 


# trade = pd.read_csv('data/seoul_realestate.csv', header = 0, skipinitialspace=True)
# trade = trade.drop(trade[trade['자치구코드'] > 20000].index)
# trade = trade.drop(['접수연도', '신고한 개업공인중개사 시군구명', '자치구코드', '법정동코드', '지번구분', '지번구분명'], axis=1)
# trade = trade.fillna(-9999)

# trade1 = pd.read_csv('temp_trade.csv', header = 0, skipinitialspace=True)
# trade2 = pd.read_csv('temp_trade2.csv', header = 0, skipinitialspace=True)
# trade3 = pd.read_csv('temp_trade3.csv', header = 0, skipinitialspace=True)
# trade1 = trade1[trade1['Unnamed: 0'] < 32360]
# tradelatlon = pd.concat([trade1, trade2, trade3])

# trade_fin = pd.merge(trade, tradelatlon, on=['자치구명', '법정동명', '본번', '부번'], how='left', suffixes=('', '_drop'))
# trade_fin = trade_fin.drop(trade_fin.filter(regex='_drop').columns, axis=1)
# trade_fin = trade_fin.rename(columns={'위도':'temp', '경도':'위도', '위도':'경도'})
# print(trade_fin.columns)

# cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액(만원)',
#         '건물면적(㎡)', '토지면적(㎡)', '권리구분', '취소일', '건축년도', '건물용도', '신고구분']
# # trade_fin = trade_fin.drop(['본번', '부번'], axis = 1)
# trade_fin = trade_fin[cols].sort_values(by="계약일")
# trade_fin = trade_fin.replace({-9999: None})
# trade_fin.to_csv("trade_fin.csv", index=False)


# USE api rent
# rent = pd.read_csv('data/seoul_rent_2024.csv', header = 0, skipinitialspace=True)
# rent = rent.fillna(-9999)
# rent = rent[~rent.duplicated(subset=['주소'])]
# tt = time.time()
# apiurl = "https://api.vworld.kr/req/address?"
# newdata = []
# count = 0
# for idx, row in rent.iterrows():
#     if idx < 125757:
#         continue
#     if row['건물용도'] != "단독다가구":
#         params = {
#             "service": "address",
#             "request": "getcoord",
#             "crs": "epsg:4326",
#             f"address": f"{row['주소']}",
#             "format": "json",
#             "type": "parcel",
#             "key": "28010AC2-7187-3642-BC8C-816A34DDF7CC"
#         }
#         try:
#             response = requests.get(apiurl, params=params)
#         except Exception as e:
#             print(e)
#             break
#         count += 1
#         if response.status_code == 200:
#             try:
#                 lat = response.json()["response"]["result"]['point']['x']
#                 lon = response.json()["response"]["result"]['point']['y']
#                 row['위도'] = lat
#                 row['경도'] = lon
#             except KeyError:
#                 print(response.json())
#                 row['위도'] = -9999
#                 row['경도'] = -9999
#         else:
#             row['위도'] = -9999
#             row['경도'] = -9999
#     else:
#         row['위도'] = -9999
#         row['경도'] = -9999
#     if idx % 100 == 0:
#         print(idx, time.time()-tt)
#         print(count)
#     if count == 8000:
#         print(count)
#         break
#     newdata.append(row)

# df = pd.DataFrame(newdata, dtype = object) 
# df = df.replace({-9999: None})
# df.to_csv(f'temp_rent4.csv') 


rent = pd.read_csv('data/seoul_rent_2024.csv', header = 0, skipinitialspace=True)
trade1 = pd.read_csv('temp_rent.csv', header = 0, skipinitialspace=True)
trade2 = pd.read_csv('temp_rent2.csv', header = 0, skipinitialspace=True)
trade3 = pd.read_csv('temp_rent3.csv', header = 0, skipinitialspace=True)
trade4 = pd.read_csv('temp_rent4.csv', header = 0, skipinitialspace=True)
tradelatlon = pd.concat([trade1, trade2, trade3])

trade_fin = pd.merge(rent, tradelatlon, on=['주소'], how='left', suffixes=('', '_drop'))
trade_fin = trade_fin.drop(trade_fin.filter(regex='_drop').columns, axis=1)
trade_fin = trade_fin.rename(columns={'위도':'temp', '경도':'위도', '위도':'경도'})
print(trade_fin.columns)

cols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세 구분', '임대면적(㎡)', '보증금(만원)',
        '임대료(만원)', '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전 보증금', '종전 임대료', '건축년도',
        '건물용도']

trade_fin = trade_fin[cols].sort_values(by="계약일")
trade_fin = trade_fin.replace({-9999: None})
trade_fin.to_csv("rent_fin.csv", index=False)