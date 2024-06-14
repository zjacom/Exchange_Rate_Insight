from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import requests
import logging
import json
import pendulum
kst = pendulum.timezone("Asia/Seoul")
dag = DAG(
<<<<<<< HEAD
    dag_id = 'getFlightCountDAG',
    start_date = datetime(2024,6,12, tzinfo=kst),
    schedule_interval= '10 0 * * *',
    catchup = False
=======
    dag_id='getFlightCountDAG',
    start_date=datetime(2024, 6, 12, tzinfo=kst),
    schedule=None,
    catchup=False
>>>>>>> fad1027b1340cb8c2ccdc43dffea614ae3f237ce
)
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS kyg8821.flight_number (
    created_at DATE,
    airportCode VARCHAR(5),
    flightCount INT DEFAULT 0
);
"""


def extract(**context):
    logging.info("Extract started")
    url = context["params"]["url"]
    airportList = context["params"]["airportName"]
    data_list = []
    for name in airportList:
        params = {'serviceKey': 'm2jY3ORe50S7ElYkebSddSiVg9TeK/ySwiH4MT2eUm1EXXLNd9aRjZu1Phv57qWBQDr4YzFny/23gir+Egof+g==',
                  'numOfRows': '100',
                  'pageNo': '1',
                  'from_time': '0000',
                  'to_time': '2400',
                  'airport': name,
                  'lang': 'K',
                  'type': 'json'}

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
    data_list = context['ti'].xcom_pull(task_ids='flightCount_extract')
    logging.info("got extract return value")

    logging.info("Transform started")
    dict_list = {}
    for data_item in data_list:
        trans_data = json.loads(data_item)
        cnt = str(trans_data["response"]["body"]["totalCount"])
        code = trans_data["response"]["body"]["items"][0]["airportCode"]
        dict_list[code] = cnt
    logging.info("Transform ended")
    return dict_list


def generate_insert_query(**context):

    dict_list = context['ti'].xcom_pull(task_ids='flightCount_transform')
    today_date = datetime.now().strftime('%Y-%m-%d')

    # pg_hook = PostgresHook(postgres_conn_id="3rd-Project")
    pg_hook = PostgresHook(postgres_conn_id='redshift_conn_id')

    for key, value in dict_list.items():
<<<<<<< HEAD
        sql_statement = f"""INSERT INTO kyg8821.flight_number (created_at, airportCode, flightCount) 
        VALUES ('{today_date}', '{key}', '{int(value)}');"""
=======
        sql_statement = f"""INSERT INTO kyg8821.flights_count (created_at, airportCode, flightCount)
        VALUES ('{today_date}', '{key}', '{value}');"""
>>>>>>> fad1027b1340cb8c2ccdc43dffea614ae3f237ce
        logging.info(sql_statement)
        pg_hook.run(sql_statement)

    logging.info("Generate is Finish")


# flight_count 테이블 생성
createFlightCountTable = PostgresOperator(
    task_id="create_flightCount_table",
    # postgres_conn_id = "3rd-Project",
    postgres_conn_id='redshift_conn_id',
    sql=CREATE_SQL,
    dag=dag)

flightCountDataExtract = PythonOperator(
    task_id='flightCount_extract',
    python_callable=extract,
    params={
        'url': 'http://apis.data.go.kr/B551177/StatusOfPassengerWorldWeatherInfo/getPassengerDeparturesWorldWeather',
        'airportName': ['KIX', 'NRT', 'FUK', 'BKK', 'HKT', 'CNX', 'SIN', 'SYD', 'BNE', 'LHR']
    },
    dag=dag)

flightCountDataTransform = PythonOperator(
    task_id='flightCount_transform',
    python_callable=transform,
    provide_context=True,
    dag=dag)

generateInsertQuery = PythonOperator(
    task_id='generate_insert_query',
    python_callable=generate_insert_query,
    provide_context=True,
    dag=dag)

createFlightCountTable >> flightCountDataExtract >> flightCountDataTransform >> generateInsertQuery
