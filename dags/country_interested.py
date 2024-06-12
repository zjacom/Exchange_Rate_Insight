from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException
from pytrends.request import TrendReq
import pendulum
import logging

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id="country_interested",
    start_date=datetime(2024, 6, 12, tzinfo=kst),
    schedule_interval='10 0 * * *',
    catchup=False,
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kyg8821.trends_metrics (
    created_at date PRIMARY KEY,
    japan int,
    australia int,
    thailand int,
    singapore int,
    england int
);
"""

def fetch_data_from_pytrends(**context):
    execution_date = context['ds']
    execution_date_dt = datetime.strptime(execution_date, '%Y-%m-%d')
    one_year_ago = execution_date_dt - timedelta(days=365)
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    pytrends = TrendReq(hl='ko-KR', tz=540)
    kw_list = ["일본 여행", "호주 여행", "태국 여행", "싱가포르 여행", "영국 여행"]
    timeframe = f'{one_year_ago_str} {execution_date}'
    pytrends.build_payload(kw_list, cat=0, timeframe=timeframe, geo='KR', gprop='')

    interest_over_time_df = pytrends.interest_over_time()
    if not interest_over_time_df.empty:
        data = interest_over_time_df.iloc[-1]
        metrics = {
            'created_at': execution_date_dt.strftime('%Y-%m-%d'),
            'japan': data['일본 여행'],
            'australia': data['호주 여행'],
            'thailand': data['태국 여행'],
            'singapore': data['싱가포르 여행'],
            'england': data['영국 여행']
        }
    else:
        # 만약 데이터를 가져오지 못했거나, 데이터가 none이면 슬랙 알람
        pass
    
    for key, value in metrics.items():
        context['task_instance'].xcom_push(key=key, value=value)


def check_latest_date(**context):
    execution_date = context['ds']
    pg_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
    latest_date_sql = "SELECT MAX(created_at) FROM kyg8821.trends_metrics;"
    latest_date = pg_hook.get_first(sql=latest_date_sql)[0]

    logging.info(execution_date)

    if latest_date and latest_date.strftime('%Y-%m-%d') == execution_date:
        logging.info(f"No data insertion needed as the latest date {latest_date} is the same as the execution date {execution_date}.")
        raise AirflowSkipException()


def generate_insert_sql(**context):
    task_instance = context['task_instance']
    metrics = {key: task_instance.xcom_pull(task_ids='fetch_data', key=key) for key in ['created_at', 'japan', 'australia', 'thailand', 'singapore', 'england']}
    
    insert_sql = f"""
    INSERT INTO kyg8821.trends_metrics (created_at, japan, australia, thailand, singapore, england)
    VALUES ('{metrics['created_at']}', {metrics['japan']}, {metrics['australia']}, {metrics['thailand']}, {metrics['singapore']}, {metrics['england']});
    """
    context['task_instance'].xcom_push(key='insert_sql', value=insert_sql)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='redshift_conn_id',
    sql=CREATE_TABLE_SQL,
    dag=dag
)

fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_pytrends,
    provide_context=True,
    dag=dag
)

check_latest = PythonOperator(
    task_id='check_latest_date',
    python_callable=check_latest_date,
    provide_context=True,
    dag=dag
)

generate_sql = PythonOperator(
    task_id='generate_sql',
    python_callable=generate_insert_sql,
    provide_context=True,
    dag=dag
)

load_data = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='redshift_conn_id',
    sql="{{ task_instance.xcom_pull(task_ids='generate_sql', key='insert_sql') }}",
    trigger_rule='all_success',
    dag=dag
)

create_table >> fetch_data >> check_latest >> generate_sql >> load_data