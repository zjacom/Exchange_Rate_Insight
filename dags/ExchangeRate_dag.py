from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import requests
import psycopg2
import logging
import json
import pendulum

kst = pendulum.timezone("Asia/Seoul")

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_conn_id')
    conn = hook.get_conn()
    conn.set_session(autocommit=False)
    return conn.cursor()


# 발급받은 인증키
authkey = "gTWRziBmxj7ZYtcT2HCGzidgXcNC787u"

searchdate = datetime.now().strftime('%Y%m%d')

# 환율 API에서 데이터 추출
def extract_data(authkey, searchdate=None):
    logging.info("Extract started")
    url = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON"
    params = {
        "authkey": 'gTWRziBmxj7ZYtcT2HCGzidgXcNC787u',
        "data": "AP01"
    }
    if searchdate:
        params["searchdate"] = searchdate

    response = requests.get(url, params=params, verify=False)
    if response.status_code == 200:
        exchange_rates = response.json()
        filtered_rates = [rate for rate in exchange_rates if rate.get('cur_unit') in ['JPY(100)', 'AUD', 'THB', 'SGD', 'EUR']]
        logging.info("Extract done")
        return filtered_rates
    else:
        logging.error(f"API 호출에 실패했습니다. Status Code: {response.status_code}")
        return None
    
# 데이터를 변환하는 함수
def transform_exchange_rate_data(**kwargs):
    ti = kwargs['ti']
    filtered_rates = ti.xcom_pull(task_ids='extract')
    searchdate = kwargs['execution_date'].strftime('%Y%m%d')


    trans_list = []
    logging.info("Transform started")
    if filtered_rates:
        df = pd.DataFrame(filtered_rates)[["cur_unit", "cur_nm", "deal_bas_r"]]
        df.rename(columns={
            "cur_unit": "currency",
            "cur_nm": "currency_name",
            "deal_bas_r": "base_rate"
        }, inplace=True)

        # base_rate 컬럼에서 쉼표 제거하고 숫자로 변환; Redshift 적재 시 오류 발생으로 추가
        df['base_rate'] = df['base_rate'].str.replace(',', '').astype(float)

        df["created_at"] = searchdate  # 날짜 컬럼 추가
        columns = ["created_at"] + [col for col in df.columns if col != "created_at"]
        df = df[columns]
        for _, row in df.iterrows():
            trans_list.append(row.to_dict())
    else:
        logging.error("API 호출에 실패했습니다.")
    logging.info("Transform ended")
    return trans_list

# 데이터를 Redshift에 적재하는 함수
def load_to_redshift(**kwargs):
    try:
        trans_list = kwargs['ti'].xcom_pull(task_ids='transform')
        logging.info("load started")

        insert_sql_template = """
        INSERT INTO kyg8821.exchange_rate (created_at, currency, currency_name, base_rate)
        VALUES (TO_DATE('{created_at}', 'YYYY-MM-DD'), '{currency}', '{currency_name}', '{base_rate}');
        """
        hook = PostgresHook(postgres_conn_id='redshift_conn_id')
        hook.run("BEGIN;")

        # Create SQL statements for each item in trans_list
        for item in trans_list:
            # None 값을 검사하여 기본값으로 대체
            created_at = item['created_at'] or 'NULL'
            currency = item['currency'] or 'NULL'
            currency_name = item['currency_name'] or 'NULL'
            base_rate = item['base_rate'] if item['base_rate'] is not None else 'NULL'

            # SQL 문 생성 후 리스트에 추가
            sql_statement = insert_sql_template.format(
                created_at=created_at,
                currency=currency,
                currency_name=currency_name,
                base_rate=base_rate
            )

            hook.run(sql_statement)

        hook.run("COMMIT;")
    
        task_instance = kwargs['ti']
        task_instance.xcom_pull(task_ids='transform')
        
    except Exception as error:
        logging.error(f"Error in generate_insert_query: {error}")
        hook.run("ROLLBACK;")

# 테이블 생성
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kyg8821.exchange_rate (
    created_at DATE PRIMARY KEY,
    currency VARCHAR(10),
    currency_name VARCHAR(30),
    base_rate FLOAT
);
"""

dag = DAG(
    dag_id = 'ExchangeRate_dag',
    description = '환율 API로부터 데이터를 가져와 Redshift에 저장하는 DAG',
    start_date = datetime(2024, 6, 12, tzinfo=kst),
    tags = ["exchange_rate"],
    catchup = True,
    schedule = '0 12 * * *',  # 매일 오후 12시에 실행
)

# 작업 정의
t0 = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='redshift_conn_id',
    sql = CREATE_TABLE_SQL,
    dag=dag,
)

t1 = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    op_kwargs={'authkey': authkey, 'searchdate': searchdate},
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform',
    python_callable=transform_exchange_rate_data,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag,
)


# 의존성 정의
t0 >> t1 >> t2 >> t3 