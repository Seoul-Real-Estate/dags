import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim

querydate = (datetime.now()+timedelta(hours=9)).strftime(r"%Y%m%d")

class realestate:
    def __init__(self, rundate, trademonth):
        self.rundate = rundate
        self.trademonth = trademonth

    def data_extract(self, url, key, origin_cols, pd_cols, make_list):
        region = [11110, 11140, 11170, 11200, 11215, 11230, 11260, 11290, 11305, 11320, 11350, 11380, 11410, 11440, 11470, 11500, 11530, 11545,
                11560, 11590, 11620 ,11650, 11680, 11710, 11740]
        region_name = ['종로구', '중구', '용산구', '성동구', '광진구', '동대문구', '중랑구', '성북구', '강북구', '도봉구', '노원구', '은평구',
                        '서대문구', '마포구', '양천구', '강서구', '구로구', '금천구', '영등포구', '동작구', '관악구', '서초구', '강남구', '송파구', '강동구']
        df = pd.DataFrame()
        for i, code in enumerate(region):
            data = []
            params ={'serviceKey' : f'{key}',
                    'LAWD_CD' : f'{code}', 
                'DEAL_YMD' : f'{self.trademonth}'}
            response = requests.get(url, params=params).content
            soup = BeautifulSoup(response, 'lxml-xml')
            rows = soup.find_all('item')
            make_list(data, rows, origin_cols)
            tempdf = pd.DataFrame(data, columns=pd_cols, dtype = object)
            tempdf["자치구명"] = region_name[i]
            df = pd.concat([df, tempdf])
        return df

    # 아파트매매
    def apartment_trade_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                name = row.find(cols[4]).get_text().strip()
            except: name = " "
            try:
                area = row.find(cols[5]).get_text().strip()
            except: area = " "
            try:
                price = row.find(cols[6]).get_text().strip().replace(',', '')
            except: price = " "
            try:
                buildyear = row.find(cols[7]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[8]).get_text().strip()
            except: addr = " "
            try:
                code = row.find(cols[9]).get_text().strip()
            except: code = " "
            try:
                floor = row.find(cols[10]).get_text().strip()
            except: floor = " "
            try:
                yndate = row.find(cols[11]).get_text().strip()
            except: yndate = " "
            try:
                type = row.find(cols[12]).get_text().strip()
            except: type = " "
            try:
                rgstdate = row.find(cols[13]).get_text().strip()
            except: rgstdate = " "
            data.append([year, month, day, dong, name, area, price, buildyear, addr, code, floor, yndate, type, rgstdate])
    
    def apartment_trade_preprocessing(self, df, cols):
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구명"] + " " + df["법정동명"] + " " + df["지번"]
        df["건물용도"] = "아파트"
        df["권리구분"] = " "
        df["토지면적"] = " "
        df = df[cols]
        df.to_csv(f'apartment_trade_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    # 오피스텔 매매
    def officetel_trade_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                name = row.find(cols[4]).get_text().strip()
            except: name = " "
            try:
                area = row.find(cols[5]).get_text().strip()
            except: area = " "
            try:
                price = row.find(cols[6]).get_text().strip().replace(',', '')
            except: price = " "
            try:
                buildyear = row.find(cols[7]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[8]).get_text().strip()
            except: addr = " "
            try:
                code = row.find(cols[9]).get_text().strip()
            except: code = " "
            try:
                floor = row.find(cols[10]).get_text().strip()
            except: floor = " "
            try:
                yndate = row.find(cols[11]).get_text().strip()
            except: yndate = " "
            try:
                type = row.find(cols[12]).get_text().strip()
            except: type = " "
            data.append([year, month, day, dong, name, area, price, buildyear, addr, code, floor, yndate, type])

    def officetel_trade_preprocessing(self, df, cols):
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구명"] + " " + df["법정동명"] + " " + df["지번"]
        df["건물용도"] = "오피스텔"
        df["권리구분"] = " "
        df["토지면적"] = " "
        df["등기일자"] = " "
        df = df[cols]
        df.to_csv(f'officetel_trade_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    # 연립다세대 매매
    def billa_trade_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                name = row.find(cols[4]).get_text().strip()
            except: name = " "
            try:
                area = row.find(cols[5]).get_text().strip()
            except: area = " "
            try:
                price = row.find(cols[6]).get_text().strip().replace(',', '')
            except: price = " "
            try:
                buildyear = row.find(cols[7]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[8]).get_text().strip()
            except: addr = " "
            try:
                code = row.find(cols[9]).get_text().strip()
            except: code = " "
            try:
                floor = row.find(cols[10]).get_text().strip()
            except: floor = " "
            try:
                yndate = row.find(cols[11]).get_text().strip()
            except: yndate = " "
            try:
                type = row.find(cols[12]).get_text().strip()
            except: type = " "
            try:
                rgstdate = row.find(cols[13]).get_text().strip()
            except: rgstdate = " "
            try:
                landarea = row.find(cols[14]).get_text().strip()
            except: landarea = " "
            data.append([year, month, day, dong, name, area, price, buildyear, addr, code, floor, yndate, type, rgstdate, landarea])

    def billa_trade_preprocessing(self, df, cols):
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구명"] + " " + df["법정동명"] + " " + df["지번"]
        df["건물용도"] = "연립다세대"
        df["권리구분"] = " "
        df = df[cols]
        df.to_csv(f'billa_trade_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    # 단독다가구 매매
    def house_trade_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                area = row.find(cols[4]).get_text().strip()
            except: area = " "
            try:
                price = row.find(cols[5]).get_text().strip().replace(',', '')
            except: price = " "
            try:
                buildyear = row.find(cols[6]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[7]).get_text().strip()
            except: addr = " "
            try:
                code = row.find(cols[8]).get_text().strip()
            except: code = " "
            try:
                yndate = row.find(cols[9]).get_text().strip()
            except: yndate = " "
            try:
                type = row.find(cols[10]).get_text().strip()
            except: type = " "
            try:
                landarea = row.find(cols[11]).get_text().strip()
            except: landarea = " "
            data.append([year, month, day, dong, area, price, buildyear, addr, code, yndate, type, landarea])

    def house_trade_preprocessing(self, df, cols):
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구명"] + " " + df["법정동명"]
        df["건물용도"] = "단독다가구"
        df["권리구분"] = " "
        df["건물명"] = " "
        df["층"] = " "
        df["등기일자"] = " "
        
        df = df[cols]
        df.to_csv(f'house_trade_{self.trademonth}_{self.rundate}.csv', index=False)
        return df



    # 아파트전월세
    def apartment_rent_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                name = row.find(cols[4]).get_text().strip()
            except: name = " "
            try:
                area = row.find(cols[5]).get_text().strip()
            except: area = " "
            try:
                uses = row.find(cols[6]).get_text().strip()
            except: uses = " "
            try:
                buildyear = row.find(cols[7]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[8]).get_text().strip()
            except: addr = " "
            try:
                type = row.find(cols[9]).get_text().strip()
            except: type = " "
            try:
                length = row.find(cols[10]).get_text().strip()
            except: length = " "
            try:
                deposit = row.find(cols[11]).get_text().strip().replace(',', '')
            except: deposit = " "
            try:
                rent = row.find(cols[12]).get_text().strip().replace(',', '')
            except: rent = " "
            try:
                before_deposit = row.find(cols[13]).get_text().strip().replace(',', '')
            except: before_deposit = " "
            try:
                before_rent = row.find(cols[14]).get_text().strip().replace(',', '')
            except: before_rent = " "
            try:
                code = row.find(cols[15]).get_text().strip()
            except: code = " "
            try:
                floor = row.find(cols[16]).get_text().strip()
            except: floor = " "
            data.append([year, month, day, dong, name, area, uses, buildyear, addr, type, length, deposit, rent, before_deposit, before_rent, code, floor])

['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '전월세구분', '임대면적', '보증금', '임대료',
            '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
['year', 'month', 'day', '법정동명', '건물명', '전용면적', '계약갱신권사용여부', '건축년도', '지번', '신규갱신여부'
                    '계약기간', '보증금', '임대료', '종전보증금', '종전임대료', '지역코드', '층']

    def apartment_rent_preprocessing(self, df, cols):
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구명"] + " " + df["법정동명"] + " " + df["지번"]
        df.loc[df["월세금액"] == "0", "전월세구분"] = "전세"
        df.loc[df["월세금액"] != "0", "전월세구분"] = "월세"
        df["건물용도"] = "아파트"
        df = df.rename(columns={'자치구': '자치구명', '법정동': '법정동명', '보증금액' : '보증금', '아파트' : '건물명', '전용면적':'임대면적', '계약구분':'신규갱신여부', 
                                '월세금액':'임대료', '종전계약보증금':'종전보증금', '종전계약월세' : '종전임대료', '갱신요구권사용':'계약갱신권사용여부'})
        df = df[cols]
        df.to_csv(f'apartment_rent_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    # 오피스텔 전월세
    def officetel_rent_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                name = row.find(cols[4]).get_text().strip()
            except: name = " "
            try:
                area = row.find(cols[5]).get_text().strip()
            except: area = " "
            try:
                uses = row.find(cols[6]).get_text().strip()
            except: uses = " "
            try:
                buildyear = row.find(cols[7]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[8]).get_text().strip()
            except: addr = " "
            try:
                type = row.find(cols[9]).get_text().strip()
            except: type = " "
            try:
                length = row.find(cols[10]).get_text().strip()
            except: length = " "
            try:
                deposit = row.find(cols[11]).get_text().strip().replace(',', '')
            except: deposit = " "
            try:
                rent = row.find(cols[12]).get_text().strip().replace(',', '')
            except: rent = " "
            try:
                before_deposit = row.find(cols[13]).get_text().strip().replace(',', '')
            except: before_deposit = " "
            try:
                before_rent = row.find(cols[14]).get_text().strip().replace(',', '')
            except: before_rent = " "
            try:
                code = row.find(cols[15]).get_text().strip()
            except: code = " "
            try:
                floor = row.find(cols[16]).get_text().strip()
            except: floor = " "
            data.append([year, month, day, dong, name, area, uses, buildyear, addr, type, length, deposit, rent, before_deposit, before_rent, code, floor])

    def officetel_rent_preprocessing(self, df, cols):
        df = df.rename(columns={'년':'year', '월':'month', '일':'day'})
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구"] + " " + df["법정동"] + " " + df["지번"]
        df.loc[df["월세"] == "0", "전월세구분"] = "전세"
        df.loc[df["월세"] != "0", "전월세구분"] = "월세"
        df["건물용도"] = "오피스텔"
        df = df.rename(columns={'자치구': '자치구명', '법정동': '법정동명', '단지' : '건물명', '전용면적':'임대면적', '계약구분':'신규갱신여부', 
                                '월세':'임대료', '종전계약보증금':'종전보증금', '종전계약월세' : '종전임대료', '갱신요구권사용':'계약갱신권사용여부'})
        df = df[cols]
        df.to_csv(f'officetel_rent_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    # 연립다세대 전월세
    def billa_rent_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                name = row.find(cols[4]).get_text().strip()
            except: name = " "
            try:
                area = row.find(cols[5]).get_text().strip()
            except: area = " "
            try:
                uses = row.find(cols[6]).get_text().strip()
            except: uses = " "
            try:
                buildyear = row.find(cols[7]).get_text().strip()
            except: buildyear = " "
            try:
                addr = row.find(cols[8]).get_text().strip()
            except: addr = " "
            try:
                type = row.find(cols[9]).get_text().strip()
            except: type = " "
            try:
                length = row.find(cols[10]).get_text().strip()
            except: length = " "
            try:
                deposit = row.find(cols[11]).get_text().strip().replace(',', '')
            except: deposit = " "
            try:
                rent = row.find(cols[12]).get_text().strip().replace(',', '')
            except: rent = " "
            try:
                before_deposit = row.find(cols[13]).get_text().strip().replace(',', '')
            except: before_deposit = " "
            try:
                before_rent = row.find(cols[14]).get_text().strip().replace(',', '')
            except: before_rent = " "
            try:
                code = row.find(cols[15]).get_text().strip()
            except: code = " "
            try:
                floor = row.find(cols[16]).get_text().strip()
            except: floor = " "
            data.append([year, month, day, dong, name, area, uses, buildyear, addr, type, length, deposit, rent, before_deposit, before_rent, code, floor])

    def billa_rent_preprocessing(self, df, cols):
        df = df.rename(columns={'년':'year', '월':'month', '일':'day'})
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구"] + " " + df["법정동"] + " " + df["지번"]
        df.loc[df["월세금액"] == "0", "전월세구분"] = "전세"
        df.loc[df["월세금액"] != "0", "전월세구분"] = "월세"
        df["건물용도"] = "연립다세대"
        df = df.rename(columns={'자치구': '자치구명', '법정동': '법정동명', '보증금액' : '보증금', '연립다세대' : '건물명', '전용면적':'임대면적', 
                                '계약구분':'신규갱신여부', '월세금액':'임대료', '종전계약보증금':'종전보증금', '종전계약월세' : '종전임대료', 
                                '갱신요구권사용':'계약갱신권사용여부'})
        df = df[cols]
        df.to_csv(f'billa_rent_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    # 단독 다가구 전월세
    def house_rent_list(self, data, source, cols):
        for row in source:
            try:
                year = row.find(cols[0]).get_text().strip()
            except: year = " "
            try:
                month = row.find(cols[1]).get_text().strip()
            except: month = " "
            try:
                day = row.find(cols[2]).get_text().strip()
            except: day
            try:
                dong = row.find(cols[3]).get_text().strip()
            except: dong = " "
            try:
                area = row.find(cols[4]).get_text().strip()
            except: area = " "
            try:
                uses = row.find(cols[5]).get_text().strip()
            except: uses = " "
            try:
                buildyear = row.find(cols[6]).get_text().strip()
            except: buildyear = " "
            try:
                type = row.find(cols[7]).get_text().strip()
            except: type = " "
            try:
                length = row.find(cols[8]).get_text().strip()
            except: length = " "
            try:
                deposit = row.find(cols[9]).get_text().strip().replace(',', '')
            except: deposit = " "
            try:
                rent = row.find(cols[10]).get_text().strip().replace(',', '')
            except: rent = " "
            try:
                before_deposit = row.find(cols[11]).get_text().strip().replace(',', '')
            except: before_deposit = " "
            try:
                before_rent = row.find(cols[12]).get_text().strip().replace(',', '')
            except: before_rent = " "
            try:
                code = row.find(cols[13]).get_text().strip()
            except: code = " "
            data.append([year, month, day, dong, area, uses, buildyear, type, length, deposit, rent, before_deposit, before_rent, code])

    def house_rent_preprocessing(self, df, cols):
        df = df.rename(columns={'년':'year', '월':'month', '일':'day'})
        df["계약일"] = pd.to_datetime(df[["year", "month", "day"]])
        df["계약일"] = df["계약일"].dt.strftime("%Y%m%d")
        df["주소"] = "서울특별시 " + df["자치구"] + " " + df["법정동"]
        df.loc[df["월세금액"] == "0", "전월세구분"] = "전세"
        df.loc[df["월세금액"] != "0", "전월세구분"] = "월세"
        df["건물용도"] = "단독다가구"
        df["건물명"] = " "
        df["층"] = " "

        df = df.rename(columns={'자치구': '자치구명', '법정동': '법정동명', '계약면적':'임대면적', '계약구분':'신규갱신여부', '보증금액': '보증금', 
                                '월세금액':'임대료', '종전계약보증금':'종전보증금', '종전계약월세' : '종전임대료', '갱신요구권사용':'계약갱신권사용여부'})
        df = df[cols]
        df.to_csv(f'house_rent_{self.trademonth}_{self.rundate}.csv', index=False)
        return df

    def get_lat_lng(self, add):
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
        


    def compare_add_latlon(self, beforedf, afterdf, fincols):
        # beforedf = pd.read_csv(beforefname, header=0)
        # afterdf = pd.read_csv(afterfname, header=0)
        newdf = pd.merge(beforedf, afterdf, how='outer', indicator=True).query('_merge == "right_only"').drop(columns=['_merge'])
        coordinates = newdf["주소"].map(self.get_lat_lng)
        newdf[["위도", "경도"]] = pd.DataFrame(coordinates.tolist(), index = newdf.index)
        findf = newdf[fincols]
        return findf
        

key = "ZU3VKtV/cyVYqylBKhohTTGbwd5/hq0d4YDqWyHz9kODNZMljBKxidxikPm6J4uY7MTEGHQfT4+FuK/UEGmNkQ=="

url = 'http://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade'
officetel_trade_origin_cols = ['dealYear','dealMonth', 'dealDay', 'umdNm', 'offiNm', 'excluUseAr', 'dealAmount', 'buildYear',
                        'jibun', 'sggCd', 'floor', 'cdealDay', 'dealingGbn']
officetel_trade_cols = ['year', 'month', 'day', '법정동명', '건물명', '전용면적', '물건금액', '건축년도', '지번', '지역코드', '층', '취소일', '신고구분']

rentcols_fin = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세구분', '임대면적', '보증금', '임대료',
            '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
rentcols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '전월세구분', '임대면적', '보증금', '임대료',
            '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전보증금', '종전임대료', '건축년도', '건물용도']
tradecols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '물건금액', '전용면적', '토지면적', '권리구분',
            '취소일', '건축년도', '건물용도', '신고구분', '등기일자']
tradecols_fin = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액', '전용면적', '토지면적', '권리구분',
            '취소일', '건축년도', '건물용도', '신고구분', '등기일자']

re = realestate('20240731', '202408')
result = re.data_extract(url, key, officetel_trade_origin_cols, officetel_trade_cols, re.officetel_trade_list)
print(result)
df = re.officetel_trade_preprocessing(result, tradecols)
# re.compare_add_latlon('apartment_rent_202407_20240724.csv', 'apartment_rent_202407_20240725.csv', rentcols_fin)