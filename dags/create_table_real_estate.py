from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'yong',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}


def get_create_table_sqls(schema):
    return {
        'naver_complex': f"""
            CREATE TABLE IF NOT EXISTS {schema}.naver_complex (
                complexNo               VARCHAR(255) PRIMARY KEY,
                complexName             VARCHAR(255),
                cortarNo                VARCHAR(255),
                realEstateTypeCode      VARCHAR(255),
                realEstateTypeName      VARCHAR(255),
                latitude                FLOAT,
                longitude               FLOAT,
                totalHouseholdCount     INTEGER,
                useApproveYmd           VARCHAR(255),
                address                 VARCHAR(255),
                maxSupplyArea           FLOAT,
                minSupplyArea           FLOAT,
                parkingPossibleCount    INTEGER,
                ParkingCountByHousehold FLOAT,
                constructionCompanyName VARCHAR(255),
                heatMethodTypeCode      VARCHAR(255),
                heatFuelTypeCode        VARCHAR(255),
                pyoengNames             VARCHAR(5000),
                managementOfficeTelNo    VARCHAR(255),
                roadZipcode             VARCHAR(255),
                road_address            VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP
            );
        """,
        'naver_realtor': f"""
            CREATE TABLE IF NOT EXISTS {schema}.naver_realtor (
                realtorId               VARCHAR(255) PRIMARY KEY,
                realtorName             VARCHAR(255),
                representativeName      VARCHAR(255),
                address                 VARCHAR(255),
                establishRegistrationNo VARCHAR(255),
                dealCount               INTEGER,
                leaseCount              INTEGER,
                rentCount               INTEGER,
                latitude                FLOAT,
                longitude               FLOAT,
                representativeTelNo     VARCHAR(255),
                cellPhoneNo             VARCHAR(255),
                cortarNo                VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP
            );
        """,
        'dabang_complex': f"""
            CREATE TABLE IF NOT EXISTS {schema}.dabang_complex (
                complex_id                  VARCHAR(255) PRIMARY KEY,
                complex_name                VARCHAR(255),
                household_num               INTEGER,
                building_approval_date_str  VARCHAR(255),
                jibun_address               VARCHAR(255),
                road_address                VARCHAR(255),
                parking_num                 INTEGER,
                parking_average             FLOAT,
                latitude                    FLOAT,
                longitude                   FLOAT,
                complex_lowest_floor         VARCHAR(255),
                complex_highest_floor        VARCHAR(255),
                build_cov_ratio             FLOAT,
                floor_area_index             FLOAT,
                provider_name               VARCHAR(255),
                heat_type_str               VARCHAR(255),
                fuel_type_str               VARCHAR(255),
                created_at                  TIMESTAMP,
                updated_at                  TIMESTAMP
            );
        """,
        'dabang_realtor': f"""
            CREATE TABLE IF NOT EXISTS {schema}.dabang_realtor (
                id          VARCHAR(255) PRIMARY KEY,
                name        VARCHAR(255),
                address     VARCHAR(255),
                agent_tel   VARCHAR(255),
                email       VARCHAR(255),
                facename    VARCHAR(255),
                latitude    FLOAT,
                longitude   FLOAT,
                reg_id      VARCHAR(255),
                users_idx   VARCHAR(255),
                greetings   VARCHAR(5000),
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP
            );
        """,
        'complex': f"""
            CREATE TABLE IF NOT EXISTS {schema}.complex (
                id              VARCHAR(255) PRIMARY KEY,
                name            VARCHAR(255),
                household_num   INTEGER,
                cortar_no       VARCHAR(255),
                region_gu       VARCHAR(255),
                region_dong     VARCHAR(255),
                address         VARCHAR(255),
                road_address    VARCHAR(255),
                latitude        FLOAT,
                longitude       FLOAT,
                parking_count   INTEGER,
                approve_date    DATE,
                provider_name   VARCHAR(255),
                heat_type       VARCHAR(255),
                heat_fuel_type  VARCHAR(255),
                platform        VARCHAR(255),
                created_at      TIMESTAMP,
                updated_at      TIMESTAMP
            );
        """,
        'realtor': f"""
            CREATE TABLE IF NOT EXISTS {schema}.realtor (
                id                      VARCHAR(255) PRIMARY KEY,
                name                    VARCHAR(255),
                representative_name     VARCHAR(255),
                cortar_no               VARCHAR(255),
                region_gu               VARCHAR(255),
                region_dong             VARCHAR(255),
                address                 VARCHAR(255),
                registration_number     VARCHAR(255),
                latitude                FLOAT,
                longitude               FLOAT,
                telephone               VARCHAR(255),
                phone                   VARCHAR(255),
                platform                VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP
            );
        """,
        'naver_real_estate': f"""
            CREATE TABLE IF NOT EXISTS {schema}.naver_real_estate (
                articleNo               VARCHAR(255) PRIMARY KEY ,
                realtorId               VARCHAR(255),
                complexNo               VARCHAR(255),
                articleName             VARCHAR(255),
                realEstateTypeName      VARCHAR(255),
                tradeTypeName           VARCHAR(255),
                floorInfo                VARCHAR(255),
                correspondingFloorCount VARCHAR(255),
                totalFloorCount         VARCHAR(255), 
                dealOrWarrantPrc        VARCHAR(255),
                rentPrc                 VARCHAR(255),
                dealPrice               FLOAT, 
                warrantPrice            FLOAT, 
                rentPrice               FLOAT,
                supply_area             INTEGER, 
                exclusive_area          INTEGER, 
                direction               VARCHAR(255),
                articleConfirmYmd        VARCHAR(255),
                articleFeatureDesc      VARCHAR(10000),
                detailDescription       VARCHAR(10000),
                tagList                 VARCHAR(1000),
                latitude                FLOAT, 
                longitude               FLOAT, 
                detailAddress           VARCHAR(255),
                exposureAddress         VARCHAR(255),
                address                 VARCHAR(255),
                roadAddress             VARCHAR(255),
                etcAddress              VARCHAR(255),
                buildingName            VARCHAR(255),
                hoNm                    VARCHAR(255),
                cortarNo                VARCHAR(255),
                parkingCount            INTEGER,
                parkingPossibleYN       VARCHAR(255),
                principalUse            VARCHAR(255),
                roomCount               INTEGER, 
                bathroomCount           INTEGER, 
                roomFacilityCodes       VARCHAR(2000),
                roomFacilities          VARCHAR(2000),
                buildingFacilityCodes   VARCHAR(2000),
                buildingFacilities      VARCHAR(2000),
                roofTopYN               VARCHAR(255),
                useApproveYmd           VARCHAR(255),
                exposeStartYMD          VARCHAR(255),
                exposeEndYMD            VARCHAR(255),
                walkingTimeToNearSubway INTEGER,
                heatMethodTypeCode      VARCHAR(255),
                heatMethodTypeName      VARCHAR(255),
                heatFuelTypeCode        VARCHAR(255),
                heatFuelTypeName        VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP,
                FOREIGN KEY (complexNo) REFERENCES {schema}.naver_complex(complexNo),
                FOREIGN KEY (realtorId) REFERENCES {schema}.naver_realtor(realtorId)
            );
        """,
        'dabang_real_estate': f"""
            CREATE TABLE IF NOT EXISTS {schema}.dabang_real_estate (
                id                          VARCHAR(255) PRIMARY KEY,
                dabang_realtor_id           VARCHAR(255),
                dabang_complex_id           VARCHAR(255),
                seq                         VARCHAR(255),
                roomTypeName                VARCHAR(255),
                roomTitle                   VARCHAR(255),
                roomDesc                    VARCHAR(255),
                priceTypeName               VARCHAR(255),
                priceTitle                  VARCHAR(255),
                latitude                    FLOAT,
                longitude                   FLOAT,
                building_approval_date_str  VARCHAR(255),
                saved_time_str              VARCHAR(255),
                room_floor_str               VARCHAR(255),
                building_floor_str           VARCHAR(255),
                direction_str               VARCHAR(255),
                address                     VARCHAR(255),
                road_address                VARCHAR(255),
                dong                        VARCHAR(255),
                ho                          VARCHAR(255),
                maintenance_cost            INTEGER,
                memo                        VARCHAR(5000),
                deal_price                  INTEGER,
                warrant_price               INTEGER,
                rent_price                  INTEGER,
                room_size                   FLOAT,
                provision_size              FLOAT,
                heating                     VARCHAR(255),
                bath_num                    INTEGER,
                beds_num                    INTEGER,
                hash_tags                   VARCHAR(255),
                parking                     VARCHAR(255),
                parking_num                 INTEGER,
                elevator_str                VARCHAR(255),
                loan_str                    VARCHAR(255),
                safeties                    VARCHAR(5000),
                room_options                VARCHAR(5000),
                contact_number              VARCHAR(255),
                created_at                  TIMESTAMP,
                updated_at                  TIMESTAMP,
                FOREIGN KEY(dabang_complex_id) REFERENCES {schema}.dabang_complex(complex_id),
                FOREIGN KEY(dabang_realtor_id) REFERENCES {schema}.dabang_realtor(id)
            );
        """,
        'real_estate': f"""
            CREATE TABLE IF NOT EXISTS {schema}.real_estate (
                id                      VARCHAR(255) PRIMARY KEY,
                complex_id              VARCHAR(255),
                realtor_id              VARCHAR(255),
                room_name               VARCHAR(255),
                room_type               VARCHAR(255),
                trade_type              VARCHAR(255),
                room_floor               VARCHAR(255),
                building_floor           VARCHAR(255),
                supply_area             FLOAT,
                exclusive_area          FLOAT,
                direction               VARCHAR(255),
                deal_price              INTEGER,
                warrant_price           INTEGER,
                rent_price              INTEGER,
                room_title              VARCHAR(10000),
                description             VARCHAR(10000),
                hash_tags               VARCHAR(2000),
                latitude                FLOAT,
                longitude               FLOAT,
                cortar_no               VARCHAR(255),
                region_gu               VARCHAR(255),
                region_dong             VARCHAR(255),
                address                 VARCHAR(500),
                road_address            VARCHAR(500),
                etc_address             VARCHAR(1000),
                ho_num                  VARCHAR(255),
                dong_num                VARCHAR(255),
                parking_count           INTEGER,
                is_parking              VARCHAR(255),
                room_count              INTEGER,
                bath_count              INTEGER,
                room_options            VARCHAR(5000),
                safe_options            VARCHAR(5000),
                approve_date            DATE,
                expose_start_date       DATE,
                heat_type               VARCHAR(255),
                heat_fuel_type          VARCHAR(255),
                platform                VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP,
                FOREIGN KEY(realtor_id) REFERENCES {schema}.realtor(id),
                FOREIGN KEY(complex_id) REFERENCES {schema}.complex(id)
            );
        """
    }


def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def create_table(create_table_sql):
    cur = get_redshift_connection()
    cur.execute(create_table_sql)


@dag(
    default_args=default_args,
    description="실매물 관련 테이블 생성 DAG",
    schedule_interval=None,
    start_date=datetime(2024, 7, 28),
    catchup=False,
    tags=['none', 'create table', 'real_estate']
)
def create_tables_real_estate():
    schema = 'raw_data'
    for table_name, create_sql in get_create_table_sqls(schema).items():
        create_task = create_table.override(task_id=f'create_{table_name}_table')(create_table_sql=create_sql)


create_tables_real_estate = create_tables_real_estate()
