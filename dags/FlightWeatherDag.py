from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import TaskInstance

from datetime import datetime, timedelta
import requests
import logging
import psycopg2
import json
import pendulum

kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id = 'getFlightWeatherDAG',
    start_date = datetime(2024,6,12, tzinfo=kst),
    schedule= None,
    # schedule_interval= '10 0 * * *',
    catchup = False
)

CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS flight_weather (
    created_at varchar(20),
    flightId varchar(10),
    airportCode varchar(5),
    humidity varchar(10),
    temp varchar(10),
    senstemp varchar(10),
    wind varchar(10)
);
"""

def extract(**context):
    logging.info("Extract started")
    url = context["params"]["url"]
    airportList = context["params"]["airportName"]
    data_list = []
    for name in airportList:
        params ={'serviceKey' : 'm2jY3ORe50S7ElYkebSddSiVg9TeK/ySwiH4MT2eUm1EXXLNd9aRjZu1Phv57qWBQDr4YzFny/23gir+Egof+g==',
                'numOfRows' : '100',
                'pageNo' : '1',
                'from_time' : '0000',
                'to_time' : '2400',
                'airport' : name,
                'lang' : 'K',
                'type' : 'json' }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            json_content = response.json()
            json_data = json.dumps(json_content, indent=4, ensure_ascii=False)
            data_list.append(json_data)
        else:
            logging.info("Error : "+response.status_code)
    logging.info("Extract done")
    return data_list

def transform(**context):
    data_list = context['ti'].xcom_pull(task_ids=f'flightWeather_extract')
    logging.info("got extract return value")
    trans_list = []
    logging.info("Transform started")
    for data_item in data_list:
        trans_data = json.loads(data_item)
        trans_list.append(trans_data)
    logging.info("Transform ended")
    return trans_list

def generate_insert_query(**context):
    try:
        trans_list = context['ti'].xcom_pull(task_ids='flightWeather_transform')
        today_date = datetime.now().strftime('%Y-%m-%d')

        # pg_hook = PostgresHook(postgres_conn_id="3rd-Project")
        pg_hook = PostgresHook(postgres_conn_id='redshift_conn_id')

        pg_hook.run("BEGIN;")

        for trans_item in trans_list:
            for item in trans_item["response"]["body"]["items"]:
                sql_statement = f"""INSERT INTO kyg8821.flight_weather (created_at, flightId, airportCode, humidity, temp, senstemp, wind)
                                   VALUES ('{today_date}', '{item['flightId']}', '{item['airportCode']}', '{item['himidity']}', '{item['temp']}', '{item['senstemp']}', '{item['wind']}');"""
                pg_hook.run(sql_statement)

        pg_hook.run("COMMIT;")

    except Exception as error:
        logging.error(f"Error in generate_insert_query: {error}")
        pg_hook.run("ROLLBACK;")
    
    logging.info("Generate is Finish")


# flight_weather 테이블 생성
createFlightWeatherTable = PostgresOperator(
    task_id = "create_flightWeather_table",
    # postgres_conn_id = "3rd-Project",
    postgres_conn_id = 'redshift_conn_id',
    sql=CREATE_QUERY,
    dag = dag)

flightWeatherDataExtract = PythonOperator(
    task_id = 'flightWeather_extract',
    python_callable = extract,
    params = {
        'url': 'http://apis.data.go.kr/B551177/StatusOfPassengerWorldWeatherInfo/getPassengerDeparturesWorldWeather',
        'airportName' : ['KIX', 'NRT', 'FUK', 'BKK', 'HKT', 'CNX', 'SIN', 'SYD', 'BNE', 'LHR']
    },
    dag = dag)

flightWeatherDataTransform = PythonOperator(
    task_id = 'flightWeather_transform',
    python_callable = transform,
    provide_context = True,
    dag = dag)

generateInsertQuery = PythonOperator(
    task_id = 'generate_insert_query',
    python_callable = generate_insert_query,
    provide_context = True,
    dag = dag)

createFlightWeatherTable >> flightWeatherDataExtract >> flightWeatherDataTransform >> generateInsertQuery 