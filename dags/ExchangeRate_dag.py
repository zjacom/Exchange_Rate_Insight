from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "kyg8821"  
    redshift_pass = "Kyg8821!1"  
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
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
        logging.info("Extract done")
        return exchange_rates
    else:
        logging.error(f"API 호출에 실패했습니다. Status Code: {response.status_code}")
        return None
    
# 데이터를 변환하는 함수
def transform_exchange_rate_data(**kwargs):
    ti = kwargs['ti']
    exchange_rates = ti.xcom_pull(task_ids='extract')
    searchdate = kwargs['execution_date'].strftime('%Y%m%d')


    trans_list = []
    logging.info("Transform started")
    if exchange_rates:
        df = pd.DataFrame(exchange_rates)[["cur_unit", "cur_nm", "deal_bas_r"]]
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
    trans_list = kwargs['ti'].xcom_pull(task_ids='transform')
    
    logging.info("load started")
    schema = "kyg8821"
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.exchange_rates;")  # 기존 데이터를 삭제
        for trans_item in trans_list:
            cur.execute("""
                INSERT INTO kyg8821.exchange_rates (created_at, currency, currency_name, base_rate)
                VALUES (%s, %s, %s, %s)
            """, (
                trans_item['created_at'],
                trans_item['currency'],
                trans_item['currency_name'],
                trans_item['base_rate']
            ))
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error inserting data: {error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
    logging.info("load done")

# 테이블 생성 함수
def create_ExchangeRate_table(cur, schema, table, drop_first):
    cur = get_Redshift_connection()
    try:
        if drop_first:
            cur.execute(f"DROP TABLE IF EXISTS kyg8821.exchange_rates;")
        create_query = f"""
            CREATE TABLE IF NOT EXISTS kyg8821.exchange_rates (
                created_at DATE,
                currency VARCHAR(10),
                currency_name VARCHAR(30),
                base_rate FLOAT
            )
        """
        cur.execute(create_query)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error creating table: {error}")
        raise
    cur.close()


dag = DAG(
    dag_id = 'ExchangeRate_dag',
    description = '환율 API로부터 데이터를 가져와 Redshift에 저장하는 DAG',
    start_date = datetime(2024, 6, 12, tzinfo=kst),
    tags = ["exchange_rate"],
    catchup = True,
    schedule = '0 12 * * *',  # 매일 오후 12시에 실행
)

# 작업 정의

t0 = PythonOperator(
    task_id='create_exchange_rate_table',
    python_callable=create_ExchangeRate_table,
    op_args=[get_Redshift_connection(), "kyg8821", "exchange_rates", True],
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
    task_id='load',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag,
)

# 의존성 정의
t0 >> t1 >> t2 >> t3
