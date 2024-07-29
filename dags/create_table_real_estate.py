from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'yong',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def get_create_table_sqls(schema):
    return {
        'naver_complex': f"""
            CREATE TABLE IF NOT EXISTS {schema}.naver_complex (
                complexNo               VARCHAR(255) PRIMARY KEY,
                complexName             VARCHAR(255),
                cortarNo                VARCHAR(255),
                address                 VARCHAR(255),
                road_address            VARCHAR(255),
                latitude                FLOAT,
                longitude               FLOAT,
                roadZipcode             VARCHAR(255),
                totalHouseholdCount     INTEGER,
                heatMethodTypeCode      VARCHAR(255),
                heatFuelTypeCode        VARCHAR(255),
                parkingPossibleCount    INTEGER,
                managementOfficeTelNo    VARCHAR(255),
                pyoengNames             VARCHAR(255),
                useApproveYmd           VARCHAR(255),
                realEstateTypeCode      VARCHAR(255),
                realEstateTypeName      VARCHAR(255),
                maxSupplyArea           FLOAT,
                minSupplyArea           FLOAT,
                constructionCompanyName VARCHAR(255),
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
                greetings   VARCHAR(255),
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP
            );
        """,
        'complex': f"""
            CREATE TABLE IF NOT EXISTS {schema}.complex (
                id              VARCHAR(255) PRIMARY KEY,
                name            VARCHAR(255),
                household_num   INTEGER,
                address         VARCHAR(255),
                road_address    VARCHAR(255),
                latitude        FLOAT,
                longitude       FLOAT,
                parking_num     INTEGER,
                approve_date    DATE,
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
                dealPrice               FLOAT, 
                warrantPrice            FLOAT, 
                rentPrc                 FLOAT, 
                floorInfo                VARCHAR(255),
                area1                   INTEGER, 
                area2                   INTEGER, 
                direction               VARCHAR(255),
                articleConfirmYmd        VARCHAR(255),
                articleFeatureDesc      VARCHAR(255),
                detailDescription       VARCHAR(255),
                tagList                 VARCHAR(255),
                buildingName            VARCHAR(255),
                latitude                FLOAT, 
                longitude               FLOAT, 
                address                 VARCHAR(255),
                roadAddress             VARCHAR(255),
                etcAddress              VARCHAR(255),
                hoNm                    VARCHAR(255),
                roomCount               INTEGER, 
                bathroomCount           INTEGER, 
                principalUse            VARCHAR(255),
                cortarNo                VARCHAR(255),
                aptUseApproveYmd        VARCHAR(255),
                parkingPossibleYN       VARCHAR(255),
                parkingCount            INTEGER,
                roomFacilityCodes       VARCHAR(255),
                roomFacilities          VARCHAR(255),
                buildingFacilityCodes   VARCHAR(255),
                buildingFacilities      VARCHAR(255),
                roofTopYN               VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP,
                FOREIGN KEY (complexNo) REFERENCES naver_complex(complexNo),
                FOREIGN KEY (realtorId) REFERENCES naver_realtor(realtorId)
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
                lat                         FLOAT,
                lng                         FLOAT,
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
                memo                        VARCHAR(255),
                dealOrWarrantPrice          INTEGER,
                rentPrice                   INTEGER,
                room_size                   FLOAT,
                provision_size              FLOAT,
                heating                     VARCHAR(255),
                bath_num                    INTEGER,
                beds_num                    INTEGER,
                hash_tags                   VARCHAR(255),
                parking_str                 VARCHAR(255),
                parking_num                 INTEGER,
                elevator_str                VARCHAR(255),
                loan_str                    VARCHAR(255),
                safeties                    VARCHAR(255),
                room_options                VARCHAR(255),
                contact_number              VARCHAR(255),
                created_at                  TIMESTAMP,
                updated_at                  TIMESTAMP,
                FOREIGN KEY(dabang_complex_id) REFERENCES dabang_complex(complex_id),
                FOREIGN KEY(dabang_realtor_id) REFERENCES dabang_realtor(id)
            );
        """,
        'real_estate': f"""
            CREATE TABLE IF NOT EXISTS {schema}.real_estate (
                id                      VARCHAR(255) PRIMARY KEY,
                realtor_id              VARCHAR(255),
                complex_id    VARCHAR(255),
                room_type               VARCHAR(255),
                trade_type              VARCHAR(255),
                room_floor               INTEGER,
                building_floor           INTEGER,
                supply_area             FLOAT,
                exclusive_area          FLOAT,
                direction               VARCHAR(255),
                description             VARCHAR(255),
                hash_tags               VARCHAR(255),
                latitude                FLOAT,
                longitude               FLOAT,
                address                 VARCHAR(255),
                road_address            VARCHAR(255),
                etc_address             VARCHAR(255),
                dong_num                VARCHAR(255),
                ho_num                  VARCHAR(255),
                room_num                INTEGER,
                bath_num                INTEGER,
                approve_date            DATE,
                parking_num             INTEGER,
                deal_price              INTEGER,
                warrant_price           INTEGER,
                rent_price              INTEGER,
                room_options            VARCHAR(255),
                room_facilitis          VARCHAR(255),
                platform                VARCHAR(255),
                created_at              TIMESTAMP,
                updated_at              TIMESTAMP,
                FOREIGN KEY(realtor_id) REFERENCES realtor(id),
                FOREIGN KEY(complex_id) REFERENCES complex(id)
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
    schema = 'wjstkddyd420'
    for table_name, create_sql in get_create_table_sqls(schema).items():
        create_task = create_table.override(task_id=f'create_{table_name}_table')(create_table_sql=create_sql)


create_tables_real_estate = create_tables_real_estate()
