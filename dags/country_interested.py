from my_slack import send_message_to_a_slack_channel, on_failure_callback
import pendulum
import logging
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from pytrends.request import TrendReq

import os
import sys
# Airflow가 실행되는 경로에서 plugins 폴더를 찾을 수 있도록 경로를 설정
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
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


def _fetch_data(**context):
    execution_date = context['ds']
    execution_date_dt = datetime.strptime(execution_date, '%Y-%m-%d')
    one_year_ago = execution_date_dt - timedelta(days=365)
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    pytrends = TrendReq(hl='ko-KR', tz=540)
    kw_list = ["일본 여행", "호주 여행", "태국 여행", "싱가포르 여행", "영국 여행"]
    timeframe = f'{one_year_ago_str} {execution_date}'
    pytrends.build_payload(
        kw_list, cat=0, timeframe=timeframe, geo='KR', gprop='')

    try:
        interest_over_time_df = pytrends.interest_over_time()
        data = interest_over_time_df.iloc[-1]
        metrics = {
            'created_at': execution_date_dt.strftime('%Y-%m-%d'),
            'japan': data['일본 여행'],
            'australia': data['호주 여행'],
            'thailand': data['태국 여행'],
            'singapore': data['싱가포르 여행'],
            'england': data['영국 여행']
        }
    except Exception as e:
        logging.info(e)
        raise AirflowFailException(e)

    for key, value in metrics.items():
        context['task_instance'].xcom_push(key=key, value=value)


def _check_latest(**context):
    execution_date = context['ds']
    pg_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
    latest_date_sql = "SELECT MAX(created_at) FROM kyg8821.trends_metrics;"
    latest_date = pg_hook.get_first(sql=latest_date_sql)[0]

    if latest_date and latest_date.strftime('%Y-%m-%d') == execution_date:
        context['task_instance'].xcom_push(
            key='skip_message', value=f"There is already data for {execution_date}.")
        return "skip_load"
    else:
        return "generate_insert_sql"


def _skip_load(**context):
    message = context['task_instance'].xcom_pull(
        task_ids='check_latest', key='skip_message')
    send_message_to_a_slack_channel(message, ":scream:")


def _generate_insert_sql(**context):
    task_instance = context['task_instance']
    metrics = {key: task_instance.xcom_pull(task_ids='fetch_data', key=key) for key in [
        'created_at', 'japan', 'australia', 'thailand', 'singapore', 'england']}

    insert_sql = f"""
    INSERT INTO kyg8821.trends_metrics (created_at, japan, australia, thailand, singapore, england)
    VALUES ('{metrics['created_at']}', {metrics['japan']}, {metrics['australia']}, {metrics['thailand']}, {metrics['singapore']}, {metrics['england']});
    """

    return insert_sql


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='redshift_conn_id',
    sql=CREATE_TABLE_SQL,
    dag=dag
)

fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=_fetch_data,
    provide_context=True,
    on_failure_callback=on_failure_callback,
    retries=3,
    retry_delay=timedelta(minutes=10),
    dag=dag
)

check_latest = BranchPythonOperator(
    task_id='check_latest',
    python_callable=_check_latest,
    provide_context=True,
    dag=dag
)

skip_load = PythonOperator(
    task_id='skip_load',
    python_callable=_skip_load,
    provide_context=True,
    dag=dag
)

generate_insert_sql = PythonOperator(
    task_id='generate_insert_sql',
    python_callable=_generate_insert_sql,
    provide_context=True,
    dag=dag
)

load_data = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='redshift_conn_id',
    sql="{{ task_instance.xcom_pull(task_ids='generate_insert_sql') }}",
    dag=dag
)

create_table >> fetch_data >> check_latest >> [skip_load, generate_insert_sql]
generate_insert_sql >> load_data
