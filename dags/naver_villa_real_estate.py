from datetime import datetime, timedelta
from airflow.decorators import task, dag, task_group
from utils.redshift_utils import *
from utils.aws_utils import *
from utils.naver_utils import *
from utils.transform_utils import *

SCHEMA = "raw_data"
SEOUL_CORTARNO = 1100000000
SI_GUN_GU_FILE_NAME = "si_gun_gu.csv"
EUP_MYEON_DONG_FILE_NAME = "eup_myeon_dong.csv"
REAL_ESTATE_NAME = "naver_real_estate"
VILLA_FILE_NAME = "naver_villa_real_estate.csv"
REALTOR_NAME = "naver_realtor"
REALTOR_FILE_NAME = "naver_villa_realtor.csv"

default_args = {
    "owner": "yong",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    description="네이버부동산 빌라/주택 데이터 수집 및 적재 DAG",
    schedule_interval="0 15 * * *",
    start_date=datetime(2024, 8, 4),
    catchup=False,
    tags=["daily", "real_estate", "naver", "villa"]
)
def naver_villa_real_estate():
    @task_group(group_id="fetch_and_process_region_data")
    def process_region():
        # 1. 시/군/구 정보 검색 API
        @task
        def process_si_gun_gu():
            if exists_s3_file(SI_GUN_GU_FILE_NAME):
                return

            df = get_region_info(SEOUL_CORTARNO)
            upload_to_s3(SI_GUN_GU_FILE_NAME, df)

        # 2. 읍/면/동 정보 검색 API
        @task
        def process_eup_myeon_dong():
            if exists_s3_file(EUP_MYEON_DONG_FILE_NAME):
                return

            eup_myeon_dong_df_list = []
            si_gun_gu_df = get_df_from_s3_csv(SI_GUN_GU_FILE_NAME)
            for cortarNo in si_gun_gu_df["cortarNo"]:
                eup_myeon_dong_df = get_region_info(cortarNo)
                eup_myeon_dong_df_list.append(eup_myeon_dong_df)

            eup_myeon_dong_df = pd.concat(eup_myeon_dong_df_list, ignore_index=True)
            upload_to_s3(EUP_MYEON_DONG_FILE_NAME, eup_myeon_dong_df)

        process_si_gun_gu() >> process_eup_myeon_dong()

    @task_group(group_id="fetch_and_process_villa_data")
    def process_villa():
        # 3. 동마다 빌라/주택 매물 리스트 검색
        @task
        def process_villa_real_estate():
            dong_df = get_df_from_s3_csv(EUP_MYEON_DONG_FILE_NAME)
            villa_df_list = []

            for cortarNo in dong_df["cortarNo"]:
                villa_df = get_villa_real_estate(cortarNo)
                villa_df_list.append(villa_df)

            villa_df = pd.concat(villa_df_list, ignore_index=True)
            today_villa_file_name = get_today_file_name(VILLA_FILE_NAME)
            upload_to_s3(today_villa_file_name, villa_df)

        # 4. 빌라/주택 새로운 매물만 추출
        @task.short_circuit
        def extract_new_villa():
            today_villa_file_name = get_today_file_name(VILLA_FILE_NAME)
            villa_df = get_df_from_s3_csv(today_villa_file_name)
            article_numbers = get_primary_keys(SCHEMA, REAL_ESTATE_NAME, "articleno")
            new_villa_df = villa_df[~villa_df["articleNo"].isin(article_numbers)]

            if new_villa_df.empty:
                return False

            upload_to_s3(today_villa_file_name, new_villa_df)
            return True

        # 5. 빌라/주택 상세정보 검색
        @task
        def process_villa_real_estate_detail():
            today_villa_file_name = get_today_file_name(VILLA_FILE_NAME)
            villa_df = get_df_from_s3_csv(today_villa_file_name)
            realtor_infos = []

            # 필요한 열 추가
            necessary_columns = [
                "complexNo", "articleName", "detailAddress", "exposureAddress", "parkingCount", "parkingPossibleYN",
                "principalUse", "useApproveYmd", "cortarNo", "roomCount", "bathroomCount", "detailDescription",
                "rentPrice", "warrantPrice", "dealPrice", "hoNm", "correspondingFloorCount", "totalFloorCount",
                "address", "roadAddress", "etcAddress", "roomFacilityCodes", "roomFacilities", "buildingFacilityCodes",
                "buildingFacilities", "roofTopYN", "exposeStartYMD", "exposeEndYMD", "walkingTimeToNearSubway",
                "heatMethodTypeCode", "heatMethodTypeName", "heatFuelTypeCode", "heatFuelTypeName", "supply_area",
                "exclusive_area"
            ]

            for column in necessary_columns:
                if column not in villa_df.columns:
                    villa_df[column] = None

            for idx, row in villa_df.iterrows():
                villa_detail_info = get_villa_real_estate_detail(row["articleNo"])
                articleDetail = villa_detail_info.get("articleDetail", {})
                articlePrice = villa_detail_info.get("articlePrice", {})
                articleFloor = villa_detail_info.get("articleFloor", {})
                landPrice = villa_detail_info.get("landPrice", {})
                articleRealtor = villa_detail_info.get("articleRealtor", {})
                articleOneroom = villa_detail_info.get("articleOneroom", {})
                articleFacility = villa_detail_info.get("articleFacility", {})

                villa_df.loc[idx, "articleName"] = articleDetail.get("articleName")
                villa_df.loc[idx, "detailAddress"] = articleDetail.get("detailAddress")
                villa_df.loc[idx, "exposureAddress"] = articleDetail.get("exposureAddress")
                villa_df.loc[idx, "parkingCount"] = articleDetail.get("parkingCount")
                villa_df.loc[idx, "parkingPossibleYN"] = articleDetail.get("parkingPossibleYN")
                villa_df.loc[idx, "principalUse"] = articleDetail.get("principalUse")
                villa_df.loc[idx, "useApproveYmd"] = articleFacility.get("buildingUseAprvYmd")
                villa_df.loc[idx, "cortarNo"] = articleDetail.get("cortarNo")
                villa_df.loc[idx, "roomCount"] = articleDetail.get("roomCount")
                villa_df.loc[idx, "bathroomCount"] = articleDetail.get("bathroomCount")
                villa_df.loc[idx, "detailDescription"] = articleDetail.get("detailDescription", '')
                villa_df.loc[idx, "rentPrice"] = articlePrice.get("rentPrice")
                villa_df.loc[idx, "warrantPrice"] = articlePrice.get("warrantPrice")
                villa_df.loc[idx, "dealPrice"] = articlePrice.get("dealPrice")
                villa_df.loc[idx, "hoNm"] = landPrice.get("hoNm")
                villa_df.loc[idx, "correspondingFloorCount"] = articleFloor.get("correspondingFloorCount")
                villa_df.loc[idx, "totalFloorCount"] = articleFloor.get("totalFloorCount")
                villa_df.loc[idx, "etcAddress"] = articleDetail.get("etcAddress")

                if articleOneroom:
                    villa_df.at[idx, "roomFacilityCodes"] = articleOneroom.get("roomFacilityCodes", '')
                    villa_df.at[idx, "roomFacilities"] = articleOneroom.get("roomFacilities", [])
                    villa_df.at[idx, "buildingFacilityCodes"] = articleOneroom.get("buildingFacilityCodes", '')
                    villa_df.at[idx, "buildingFacilities"] = articleOneroom.get("buildingFacilities", [])
                    villa_df.loc[idx, "roofTopYN"] = articleOneroom.get("roofTopYN", '')
                else:
                    villa_df.at[idx, "roomFacilityCodes"] = articleFacility.get("lifeFacilityList", '')
                    villa_df.at[idx, "roomFacilities"] = articleFacility.get("lifeFacilities", [])
                    villa_df.at[idx, "buildingFacilityCodes"] = articleFacility.get("securityFacilityList", '')
                    villa_df.at[idx, "buildingFacilities"] = articleFacility.get("securityFacilities", [])

                villa_df.loc[idx, "exposeStartYMD"] = articleDetail.get("exposeStartYMD")
                villa_df.loc[idx, "exposeEndYMD"] = articleDetail.get("exposeEndYMD")
                villa_df.loc[idx, "walkingTimeToNearSubway"] = articleDetail.get("walkingTimeToNearSubway")
                villa_df.loc[idx, "heatMethodTypeCode"] = articleFacility.get("heatMethodTypeCode", '')
                villa_df.loc[idx, "heatMethodTypeName"] = articleFacility.get("heatMethodTypeName", '')
                villa_df.loc[idx, "heatFuelTypeCode"] = articleFacility.get("heatFuelTypeCode", '')
                villa_df.loc[idx, "heatFuelTypeName"] = articleFacility.get("heatFuelTypeName", '')

                # 공인중개사 값 추가
                realtor_infos.append(articleRealtor)

            upload_to_s3(today_villa_file_name, villa_df)

            realtor_df = pd.DataFrame(realtor_infos)
            realtor_df = realtor_df.drop_duplicates(subset="realtorId")
            today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
            upload_to_s3(today_realtor_file_name, realtor_df)

        process_villa_real_estate() >> extract_new_villa() >> process_villa_real_estate_detail()

    # 6. 빌라/주택 전처리
    @task
    def transform_villa_real_estate():
        try:
            today_villa_file_name = get_today_file_name(VILLA_FILE_NAME)
            villa_df = get_df_from_s3_csv(today_villa_file_name)

            # 정수형 필드에 문자열이 올 경우 NaN 변환
            clean_numeric_column(villa_df, "roomCount")
            clean_numeric_column(villa_df, "bathroomCount")
            clean_numeric_column(villa_df, "dealPrice")
            clean_numeric_column(villa_df, "warrantPrice")
            clean_numeric_column(villa_df, "rentPrice")

            villa_df["rentPrc"] = villa_df["rentPrc"].fillna(0)
            villa_df["supply_area"] = villa_df["area1"].fillna(0).astype(int)
            villa_df["exclusive_area"] = villa_df["area2"].fillna(0).astype(int)
            villa_df["parkingCount"] = villa_df["parkingCount"].fillna(0).astype(int)
            villa_df["roomCount"] = villa_df["roomCount"].fillna(0).astype(int)
            villa_df["bathroomCount"] = villa_df["bathroomCount"].fillna(0).astype(int)
            villa_df["walkingTimeToNearSubway"] = villa_df["walkingTimeToNearSubway"].fillna(0).astype(int)
            villa_df["created_at"] = datetime.now()
            villa_df["updated_at"] = datetime.now()
        except FileNotFoundError:
            logging.error(f"Error File '{today_villa_file_name}' not found in S3")
            raise
        except pd.errors.EmptyDataError:
            logging.error(f"Error File '{today_villa_file_name}' is empty or could not be parsed.")
            raise
        except ValueError as ve:
            logging.error(f"ValueError during transformation: {ve}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error in transform_villa_real_estate: {e}")
            raise

        # 컬럼 순서 맞추기
        desired_columns = ["articleNo", "realtorId", "complexNo", "articleName", "realEstateTypeName", "tradeTypeName",
                           "floorInfo", "correspondingFloorCount", "totalFloorCount", "dealOrWarrantPrc", "rentPrc",
                           "dealPrice", "warrantPrice", "rentPrice", "supply_area", "exclusive_area", "direction",
                           "articleConfirmYmd", "articleFeatureDesc", "detailDescription", "tagList", "latitude",
                           "longitude", "detailAddress", "exposureAddress", "address", "roadAddress", "etcAddress",
                           "buildingName", "hoNm", "cortarNo", "parkingCount", "parkingPossibleYN", "principalUse",
                           "roomCount", "bathroomCount", "roomFacilityCodes", "roomFacilities", "buildingFacilityCodes",
                           "buildingFacilities", "roofTopYN", "useApproveYmd", "exposeStartYMD", "exposeEndYMD",
                           "walkingTimeToNearSubway", "heatMethodTypeCode", "heatMethodTypeName", "heatFuelTypeCode",
                           "heatFuelTypeName", "created_at", "updated_at"
                           ]
        villa_df = villa_df[desired_columns]
        today_transform_villa_file_name = get_today_file_name("transform_" + VILLA_FILE_NAME)
        upload_to_s3(today_transform_villa_file_name, villa_df)

    # 7. 빌라/주택 redshift 적재
    @task
    def load_to_redshift_villa():
        today_transform_villa_file_name = get_today_file_name("transform_" + VILLA_FILE_NAME)
        s3_path = f"s3://team-ariel-2-data/data/{today_transform_villa_file_name}"
        load_to_redshift_from_s3_csv(SCHEMA, REAL_ESTATE_NAME, s3_path)

    # 8. 새로운 공인중개사 추출
    @task.short_circuit
    def extract_new_realtor():
        today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
        realtor_df = get_df_from_s3_csv(today_realtor_file_name)
        realtor_ids = get_primary_keys(SCHEMA, REALTOR_NAME, "realtorid")
        new_realtor_df = realtor_df[~realtor_df["realtorId"].isin(realtor_ids)]
        if new_realtor_df.empty:
            return False

        upload_to_s3(today_realtor_file_name, new_realtor_df)
        return True

    # 9. 공인중개사 전처리
    @task
    def transform_realtor():
        today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
        realtor_df = get_df_from_s3_csv(today_realtor_file_name)
        desired_columns = ["realtorId", "realtorName", "representativeName", "address", "establishRegistrationNo",
                           "dealCount", "leaseCount", "rentCount", "latitude", "longitude", "representativeTelNo",
                           "cellPhoneNo", "cortarNo"]
        realtor_df = realtor_df[desired_columns]
        realtor_df = realtor_df[realtor_df["realtorId"].notna()]
        realtor_df["dealCount"] = realtor_df["dealCount"].fillna(0).astype(int)
        realtor_df["leaseCount"] = realtor_df["leaseCount"].fillna(0).astype(int)
        realtor_df["rentCount"] = realtor_df["rentCount"].fillna(0).astype(int)
        realtor_df["created_at"] = datetime.now()
        realtor_df["updated_at"] = datetime.now()
        today_transform_realtor_file_name = get_today_file_name("transform_" + REALTOR_FILE_NAME)
        upload_to_s3(today_transform_realtor_file_name, realtor_df)

    # 10. 공인중개사 redshift 적재
    @task
    def load_to_redshift_realtor():
        today_transform_realtor_file_name = get_today_file_name("transform_" + REALTOR_FILE_NAME)
        s3_path = f"s3://team-ariel-2-data/data/{today_transform_realtor_file_name}"
        load_to_redshift_from_s3_csv(SCHEMA, REALTOR_NAME, s3_path)

    region = process_region()
    process_villa = process_villa()
    transform_villa_task = transform_villa_real_estate()
    load_to_redshift_villa_task = load_to_redshift_villa()
    extract_new_realtor_task = extract_new_realtor()
    transform_realtor = transform_realtor()
    load_to_redshift_realtor_task = load_to_redshift_realtor()

    region >> process_villa
    process_villa >> transform_villa_task >> load_to_redshift_villa_task
    process_villa >> extract_new_realtor_task >> transform_realtor >> load_to_redshift_realtor_task


naver_villa_real_estate = naver_villa_real_estate()
