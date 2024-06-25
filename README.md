# 환율에 따른 여행 관련 데이터
----
# 프로젝트 개요

이 프로젝트는 환율과 다양한 여행 데이터를 보여주는 대시보드입니다. 

날짜와 국가에 따른 환율과 트렌드 지표를 보여주고, 해당 국가의 공항으로 가는 항공편 수, 항공 도착지의 온도, 습도와 같은 날씨 정보를 제공합니다.


# 프로젝트 목표

- 데이터 파이프라인 구축하기
- 주기적으로 업데이트 되는 데이터에 대한 DAG 작성
- Airflow 상의 개발과 운영 경험
- 데이터 파이프라인에서 데이터 웨어하우스(redshift) 사용 방법 익히기

# 팀원 및 역할
- 김승훈 → Airflow 서버 환경 구축, CI/CD 구축, 트렌드 지수 ETL 작성 및 시각화
- 문소정 → 항공편과 도착지에 대한 날씨 데이터 ETL, 공항에 따른 항공편 수 데이터 ETL , DAG 작성 및 시각화
- 이소윤 → 환율 데이터 ETL, DAG 작성 및 대시보드 시각화

# 아키텍처 구조
<img width="946" alt="스크린샷 2024-06-25 오후 3 39 57" src="https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/b078a3ee-8d2f-41c4-9516-64ec6d2c139e">

# 프로세스 설명

## Data preprocessing

### 데이터 출처

- 트렌드 지표 API
  - https://trends.google.com/trends/
- 한국수출입은행 환율 API
  - https://www.koreaexim.go.kr/ir/HPHKIR019M01
- 인천국제공항공사_기상정보 API
    - https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15095086

## Data Visualize / Final Dashboard Result
### Dataset 및 차트 생성
- 국가별 트렌드 지표
  ![Untitled](https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/09d364a6-afe3-4104-9604-fcc48449b003)

  
- 환율

  ![Untitled](https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/3e9d4802-3f08-4544-ab8f-8a405c19b52c)

  
- 날짜, 공항별 항공편 수
  
  ![Untitled](https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/371deda4-e64c-4ba9-8d0c-bdd4a8e44acc)


- 날짜, 공항별 체감온도와 풍속
  
  ![Untitled](https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/24c13be3-2f2d-4c6b-b4eb-672cd129faf7)

  
- 날짜, 공항별 습도와 온도
  
  ![Untitled](https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/aa6ec504-b962-4a15-b341-203a2515ca9b)


### 대시보드 구성 및 필터 생성
- 전체 대시보드
  ![Untitled](https://github.com/zjacom/Exchange_Rate_Insight/assets/112957047/9e669a0f-c004-46ea-ad3b-03655c2c9fa6)

## 활용 기술
### 언어
<img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=Python&logoColor=white"> <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=PostgreSQL&logoColor=white"> <img src="https://img.shields.io/badge/YAML-CB171E?style=flat&logo=YAML&logoColor=white"> 

### 대시보드
<img src="https://img.shields.io/badge/Superset-404040?style=flat&logo=Superset&logoColor=white">

### 컨테이너
<img src="https://img.shields.io/badge/Docker-2496ED?style=flat&logo=Docker&logoColor=white">

### 데이터 웨어하우스
<img src="https://img.shields.io/badge/googlecloud-8C4FFF?style=flat&logo=googlecloud&logoColor=white">

### 클라우드
<img src="https://img.shields.io/badge/Amazon Redshift-4285F4?style=flat&logo=Amazon Redshift&logoColor=white">

### 데이터 파이프라인
<img src="https://img.shields.io/badge/apacheairflow-017CEE?style=flat&logo=apacheairflow&logoColor=white">

### CI/CD
<img src="https://img.shields.io/badge/githubactions-2088FF?style=flat&logo=githubactions&logoColor=white">

### 커뮤니케이션 & 협업
<img src="https://img.shields.io/badge/GitHub-181717?style=flat&logo=GitHub&logoColor=white"> <img src="https://img.shields.io/badge/Gather-2535A0?style=flat&logo=Gather&logoColor=white"> <img src="https://img.shields.io/badge/Slack-4A154B?style=flat&logo=Slack&logoColor=white"> <img src="https://img.shields.io/badge/Notion-000000?style=flat&logo=Notion&logoColor=white">

# 참여자 정보
<table>
  <tbody>
    <tr>
    <td align="center"><a href="https://github.com/zjacom"><img src="https://avatars.githubusercontent.com/u/112957047?v=4" width="100px;" alt=""/><br /><sub><b>김승훈</b></sub></a><br /></td>
      <td align="center"><a href="https://github.com/Elisha0510"><img src="https://avatars.githubusercontent.com/u/82575174?v=4" width="100px;" alt=""/><br /><sub><b>문소정</b></sub></a><br /></td>
      <td align="center"><a href="https://github.com/soyouu"><img src="https://avatars.githubusercontent.com/u/132499783?v=4" width="100px;" alt=""/><br /><sub><b>이소윤</b></sub></a><br /></td>
    </tr>
  </tbody>
</table>
