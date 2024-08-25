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
ONE_ROOM_FILE_NAME = "naver_one_room_real_estate.csv"
REALTOR_NAME = "naver_realtor"
REALTOR_FILE_NAME = "naver_one_room_realtor.csv"

default_args = {
    "owner": "yong",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def add_necessary_columns(df, columns):
    for column in columns:
        if column not in df.columns:
            df[column] = None


def add_one_room_detail_columns(idx, one_room_df, one_room_detail_info):
    try:
        articleDetail = one_room_detail_info.get("articleDetail", {})
        articleFacility = one_room_detail_info.get("articleFacility")
        articlePrice = one_room_detail_info.get("articlePrice", {})
        articleFloor = one_room_detail_info.get("articleFloor", {})
        landPrice = one_room_detail_info.get("landPrice", {})
        articleOneroom = one_room_detail_info.get("articleOneroom", {})

        one_room_df.loc[idx, "articleName"] = articleDetail.get("articleName")
        one_room_df.loc[idx, "detailAddress"] = articleDetail.get("detailAddress", "")
        one_room_df.loc[idx, "exposureAddress"] = articleDetail.get("exposureAddress", "")
        one_room_df.loc[idx, "parkingCount"] = articleDetail.get("parkingCount")
        one_room_df.loc[idx, "parkingPossibleYN"] = articleDetail.get("parkingPossibleYN")
        one_room_df.loc[idx, "principalUse"] = articleDetail.get("principalUse")
        one_room_df.loc[idx, "cortarNo"] = articleDetail.get("cortarNo")
        one_room_df.loc[idx, "roomCount"] = articleDetail.get("roomCount")
        one_room_df.loc[idx, "bathroomCount"] = articleDetail.get("bathroomCount")
        one_room_df.loc[idx, "detailDescription"] = articleDetail.get("detailDescription", "")
        one_room_df.loc[idx, "etcAddress"] = articleDetail.get("etcAddress")
        one_room_df.loc[idx, "exposeStartYMD"] = articleDetail.get("exposeStartYMD")
        one_room_df.loc[idx, "exposeEndYMD"] = articleDetail.get("exposeEndYMD")
        one_room_df.loc[idx, "walkingTimeToNearSubway"] = articleDetail.get("walkingTimeToNearSubway")
        one_room_df.loc[idx, "rentPrice"] = articlePrice.get("rentPrice")
        one_room_df.loc[idx, "warrantPrice"] = articlePrice.get("warrantPrice")
        one_room_df.loc[idx, "dealPrice"] = articlePrice.get("dealPrice")
        one_room_df.loc[idx, "hoNm"] = landPrice.get("hoNm")
        one_room_df.loc[idx, "correspondingFloorCount"] = articleFloor.get("correspondingFloorCount")
        one_room_df.loc[idx, "totalFloorCount"] = articleFloor.get("totalFloorCount")
        if articleFacility:
            one_room_df.loc[idx, "useApproveYmd"] = articleFacility.get("buildingUseAprvYmd", "")
            one_room_df.loc[idx, "heatMethodTypeCode"] = articleFacility.get("heatMethodTypeCode", "")
            one_room_df.loc[idx, "heatMethodTypeName"] = articleFacility.get("heatMethodTypeName", "")
            one_room_df.loc[idx, "heatFuelTypeCode"] = articleFacility.get("heatFuelTypeCode", "")
            one_room_df.loc[idx, "heatFuelTypeName"] = articleFacility.get("heatFuelTypeName", "")
            one_room_df.at[idx, "roomFacilityCodes"] = articleFacility.get("lifeFacilityList", "")
            one_room_df.at[idx, "roomFacilities"] = articleFacility.get("lifeFacilities", [])
            one_room_df.at[idx, "buildingFacilityCodes"] = articleFacility.get("securityFacilityList", "")
            one_room_df.at[idx, "buildingFacilities"] = articleFacility.get("securityFacilities", [])

        if articleOneroom:
            one_room_df.at[idx, "roomFacilityCodes"] = articleOneroom.get("roomFacilityCodes", "")
            one_room_df.at[idx, "roomFacilities"] = articleOneroom.get("roomFacilities", [])
            one_room_df.at[idx, "buildingFacilityCodes"] = articleOneroom.get("buildingFacilityCodes", "")
            one_room_df.at[idx, "buildingFacilities"] = articleOneroom.get("buildingFacilities", [])
            one_room_df.loc[idx, "roofTopYN"] = articleOneroom.get("roofTopYN", "")
    except Exception as e:
        logging.error(f"Error Method is [add_one_room_detail_columns]: {e}")
        raise


def transform_one_room_df(one_room_df):
    try:
        clean_numeric_column(one_room_df, "roomCount")
        clean_numeric_column(one_room_df, "bathroomCount")
        clean_numeric_column(one_room_df, "dealPrice")
        clean_numeric_column(one_room_df, "warrantPrice")
        clean_numeric_column(one_room_df, "rentPrice")
        one_room_df.loc[:, "rentPrc"] = one_room_df["rentPrc"].fillna(0)
        one_room_df["supply_area"] = one_room_df["area1"].fillna(0).astype(int)
        one_room_df["exclusive_area"] = one_room_df["area2"].fillna(0).astype(int)
        one_room_df["parkingCount"] = one_room_df["parkingCount"].fillna(0).astype(int)
        one_room_df["roomCount"] = one_room_df["roomCount"].fillna(0).astype(int)
        one_room_df["bathroomCount"] = one_room_df["bathroomCount"].fillna(0).astype(int)
        one_room_df["walkingTimeToNearSubway"] = one_room_df["walkingTimeToNearSubway"].fillna(0).astype(int)
        one_room_df["created_at"] = datetime.now()
        one_room_df["updated_at"] = datetime.now()
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
        one_room_df = one_room_df[desired_columns]
        return one_room_df
    except Exception as e:
        logging.error(f"Error transform one_room_df: {e}")
        raise


@dag(
    default_args=default_args,
    description="네이버부동산 원룸/투룸 데이터 수집 및 적재 DAG",
    schedule_interval="0 17 * * *",
    start_date=datetime(2024, 8, 5),
    catchup=False,
    tags=["daily", "real_estate", "naver", "one_room"]
)
def naver_one_room_real_estate():
    @task_group(
        group_id="fetch_and_process_region_data",
        tooltip="서울 전지역 읍면동 정보 저장"
    )
    def fetch_and_process_region_data():
        @task
        def fetch_si_gun_gu():
            if exists_s3_file(SI_GUN_GU_FILE_NAME):
                return

            df = get_region_info(SEOUL_CORTARNO)
            upload_to_s3(SI_GUN_GU_FILE_NAME, df)

        @task
        def fetch_eup_myeon_dong():
            if exists_s3_file(EUP_MYEON_DONG_FILE_NAME):
                return

            eup_myeon_dong_df_list = []
            si_gun_gu_df = get_df_from_s3_csv(SI_GUN_GU_FILE_NAME)
            for cortarNo in si_gun_gu_df["cortarNo"]:
                eup_myeon_dong_df = get_region_info(cortarNo)
                eup_myeon_dong_df_list.append(eup_myeon_dong_df)

            eup_myeon_dong_df = pd.concat(eup_myeon_dong_df_list, ignore_index=True)
            upload_to_s3(EUP_MYEON_DONG_FILE_NAME, eup_myeon_dong_df)

        fetch_si_gun_gu() >> fetch_eup_myeon_dong()

    @task_group(
        group_id="fetch_and_process_one_room_data",
        tooltip="원룸/투룸 새로운 매물 및 공인중개사 정보 CSV 저장"
    )
    def fetch_and_process_one_room_data():
        @task
        def fetch_one_room_real_estate():
            dong_df = get_df_from_s3_csv(EUP_MYEON_DONG_FILE_NAME)
            one_room_df_list = []

            for cortarNo in dong_df["cortarNo"]:
                one_room_df = get_one_room_real_estate(cortarNo)
                one_room_df_list.append(one_room_df)

            one_room_df = pd.concat(one_room_df_list, ignore_index=True)
            today_one_room_file_name = get_today_file_name(ONE_ROOM_FILE_NAME)
            upload_to_s3(today_one_room_file_name, one_room_df)

        @task.short_circuit
        def extract_new_one_room():
            today_one_room_file_name = get_today_file_name(ONE_ROOM_FILE_NAME)
            one_room_df = get_df_from_s3_csv(today_one_room_file_name)
            article_numbers = get_primary_keys(SCHEMA, REAL_ESTATE_NAME, "articleno")
            new_one_room_df = one_room_df[~one_room_df["articleNo"].isin(article_numbers)]

            if new_one_room_df.empty:
                return False

            upload_to_s3(today_one_room_file_name, new_one_room_df)
            return True

        @task
        def fetch_one_room_real_estate_detail():
            today_one_room_file_name = get_today_file_name(ONE_ROOM_FILE_NAME)
            one_room_df = get_df_from_s3_csv(today_one_room_file_name)
            realtor_infos = []
            necessary_columns = [
                "complexNo", "articleName", "detailAddress", "exposureAddress", "parkingCount", "parkingPossibleYN",
                "principalUse", "useApproveYmd", "cortarNo", "roomCount", "bathroomCount", "detailDescription",
                "rentPrice", "warrantPrice", "dealPrice", "hoNm", "correspondingFloorCount", "totalFloorCount",
                "address", "roadAddress", "etcAddress", "roomFacilityCodes", "roomFacilities", "buildingFacilityCodes",
                "buildingFacilities", "roofTopYN", "exposeStartYMD", "exposeEndYMD", "walkingTimeToNearSubway",
                "heatMethodTypeCode", "heatMethodTypeName", "heatFuelTypeCode", "heatFuelTypeName", "supply_area",
                "exclusive_area"
            ]
            add_necessary_columns(one_room_df, necessary_columns)
            for idx, row in one_room_df.iterrows():
                one_room_detail_info = get_one_room_real_estate_detail(row["articleNo"])
                add_one_room_detail_columns(idx, one_room_df, one_room_detail_info)
                articleRealtor = one_room_detail_info.get("articleRealtor", {})
                realtor_infos.append(articleRealtor)

            upload_to_s3(today_one_room_file_name, one_room_df)

            realtor_df = pd.DataFrame(realtor_infos)
            realtor_df = realtor_df.drop_duplicates(subset="realtorId")
            today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
            upload_to_s3(today_realtor_file_name, realtor_df)

        fetch_one_room_real_estate() >> extract_new_one_room() >> fetch_one_room_real_estate_detail()

    @task
    def transform_one_room_real_estate():
        today_one_room_file_name = get_today_file_name(ONE_ROOM_FILE_NAME)
        one_room_df = get_df_from_s3_csv(today_one_room_file_name)
        one_room_df = transform_one_room_df(one_room_df)
        today_transform_one_room_file_name = get_today_file_name("transform_" + ONE_ROOM_FILE_NAME)
        upload_to_s3(today_transform_one_room_file_name, one_room_df)

    @task
    def load_to_redshift_one_room():
        today_transform_one_room_file_name = get_today_file_name("transform_" + ONE_ROOM_FILE_NAME)
        s3_path = f"s3://team-ariel-2-data/data/{today_transform_one_room_file_name}"
        load_to_redshift_from_s3_csv(SCHEMA, REAL_ESTATE_NAME, s3_path)

    @task.short_circuit
    def extract_new_realtor():
        today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
        realtor_df = get_df_from_s3_csv(today_realtor_file_name)
        realtor_ids = get_primary_keys(SCHEMA, REAL_ESTATE_NAME, "articleno")
        new_realtor_df = realtor_df[~realtor_df["realtorId"].isin(realtor_ids)]
        if new_realtor_df.empty:
            return False

        upload_to_s3(today_realtor_file_name, new_realtor_df)
        return True

    @task
    def transform_realtor():
        today_realtor_file_name = get_today_file_name(REALTOR_FILE_NAME)
        realtor_df = get_df_from_s3_csv(today_realtor_file_name)
        realtor_df = realtor_df[realtor_df['realtorId'].notna()]
        desired_columns = ["realtorId", "realtorName", "representativeName", "address", "establishRegistrationNo",
                           "dealCount", "leaseCount", "rentCount", "latitude", "longitude", "representativeTelNo",
                           "cellPhoneNo", "cortarNo"]
        realtor_df = realtor_df[desired_columns]
        realtor_df["dealCount"] = realtor_df["dealCount"].fillna(0).astype(int)
        realtor_df["leaseCount"] = realtor_df["leaseCount"].fillna(0).astype(int)
        realtor_df["rentCount"] = realtor_df["rentCount"].fillna(0).astype(int)
        realtor_df["created_at"] = datetime.now()
        realtor_df["updated_at"] = datetime.now()
        today_transform_realtor_file_name = get_today_file_name("transform_" + REALTOR_FILE_NAME)
        upload_to_s3(today_transform_realtor_file_name, realtor_df)

    @task
    def load_to_redshift_realtor():
        today_transform_realtor_file_name = get_today_file_name("transform_" + REALTOR_FILE_NAME)
        s3_path = f"s3://team-ariel-2-data/data/{today_transform_realtor_file_name}"
        load_to_redshift_from_s3_csv(SCHEMA, REALTOR_NAME, s3_path)

    fetch_region = fetch_and_process_region_data()
    fetch_one_room = fetch_and_process_one_room_data()
    transform_one_room = transform_one_room_real_estate()
    load_to_redshift_one_room = load_to_redshift_one_room()
    extract_new_realtor = extract_new_realtor()
    transform_realtor = transform_realtor()
    load_to_redshift_realtor = load_to_redshift_realtor()

    fetch_region >> fetch_one_room
    fetch_one_room >> transform_one_room >> load_to_redshift_one_room
    fetch_one_room >> extract_new_realtor >> transform_realtor >> load_to_redshift_realtor


naver_one_room_real_estate = naver_one_room_real_estate()
