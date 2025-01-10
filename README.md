# 서울시 부동산과 인프라 통합 정보 대시보드

<br>

## 1. 프로젝트 개요
기존 부동산 서비스가 한계점이 있다고 판단해 부동산 거래 내역 및 그 주변 인프라 정보를 종합적으로 제공하는 대시보드의 필요성을 느껴 대시보드를 구현하고자 함. 이를 통해 개인의 선호에 맞는 부동산 선택 경험을 제공하고자 함.

<br>

## 2. 프로젝트 구조
![image](https://github.com/user-attachments/assets/a5311500-4031-4c5b-9cf4-9bf2f16417b3)

<br>

## 3. 기술 스택
- Python, SQL
- Pandas, BeautifulSoup, Requests, Selenium
- Docker, Airflow, AWS S3, AWS Redshift, AWS RDS, AWS Elasticache Redis
- Tableau Cloud
- Git, GitHub, Notion, Slack, GatherTown

<br>

## 4. 프로젝트 팀 구성 및 역할
|이름|역할|
|-------|------------|
|문소정|ERD 설계,인프라(병원, 문화공간, 산과 공원, 등록인구, 가구 수, 버스, 지하철) 데이터 수집 및 ETL 파이프라인 구축, 데이터 시각화|
|이주헌|ERD 설계,부동산 실거래 데이터 ETL 파이프라인 구축|
|이풍훈|ERD 설계, AWS Infra 구축, CI/CD 구성, Airflow 관리, 데이터 시각화|
|전상용|ERD 설계, 네이버 부동산과 다방 매물 데이터 수집 및 ETL 파이프라인 구축, 데이터 시각화|
|정가인|ERD 설계, 인프라(학교, 학원, 어린이집, 영화관) 데이터 수집 및 ETL 파이프라인 구축, 인프라 데이터 통합, 시각화|

<br>

## 5. 구현 결과

- 실매물
  ![image](https://github.com/user-attachments/assets/9b612d18-9d01-4fee-a398-a7fe90e88deb)

- 네이버 부동산 & 다방 통계
  ![image](https://github.com/user-attachments/assets/2c068645-aa64-4592-abb2-a5eec9a93840)

- 부동산 실거래 조회
  ![image](https://github.com/user-attachments/assets/8ef83387-e97c-4cae-9936-89c19c37f9f7)

- 자치구 서비스 업종별 점포 수
  ![image](https://github.com/user-attachments/assets/c174ddfa-2c4a-44b7-8a60-a678d7c724cc)

- 교육 통계
  ![image](https://github.com/user-attachments/assets/bf772a00-d878-4675-8ac3-bb0fffc2dcff)

- 매물과 인프라의 위치
  ![image](https://github.com/user-attachments/assets/cfb2efc0-39f8-46c0-a1ed-31f565119bf8)

- 서울시 등록인구 통계
  ![image](https://github.com/user-attachments/assets/358d4c10-3446-48ec-9aa3-3e6bbafd6a18)

