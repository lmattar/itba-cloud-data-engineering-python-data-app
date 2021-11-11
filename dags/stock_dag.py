"""Dummy DAG."""
import matplotlib
matplotlib.use('Agg')

import os
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import sqlalchemy
from postgresClient import PostgresClient
from models import create_tables
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "PCQVY7EY4NVW7AGD"
STOCK_FN = "TIME_SERIES_DAILY"
DB_NAME = "stockdb"
DB_HOST = "pg_container"
DB_PORT = "5432"
POSTGRES_USER = "postgres"
POSTGRES_PASS = "postgres"
STOCKS = {"google": "goog", "microsoft": "msft", "amazon": "amzn"}
SQL_TABLE = """ CREATE TABLE IF NOT EXISTS stock 
            ( id SERIAL  
            , symbol character varying
            , date date
            , open numeric
            , high numeric
            , low numeric
            , close  numeric 
             , PRIMARY KEY (id)
               )"""

def if_not_exists_create_tables(**context):
    pc = PostgresClient(DB_NAME, POSTGRES_USER, POSTGRES_PASS, DB_HOST, DB_PORT)
    pc.execute(SQL_TABLE)
    # create_tables(pc._get_engine)


def get_stock_data(stock_symbol, **context):

    date = context["ds"]  # f"{date:%Y-%m-%d}"  # read execution date from context
    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")

    # r = requests.get(end_point)
    # data = json.loads(r.content)
    data = {
        "date": ["07/11/2021"],
        "symbol": [stock_symbol],
        "low": [10],
        "high": ["30"],
    }
    df = pd.DataFrame(data)
    # df = (
    #     pd.DataFrame(data)
    #     .T.reset_index()
    #     .rename(columns={'index': 'date'})
    # )

    # 4df = df[df['date'] == date]

    return df.to_json()


def _insert_stock_in_database(**context):
    task_instance = context["ti"]

    dfs = []
    for ticker in STOCKS:

        stock_df = pd.read_json(
            task_instance.xcom_pull(task_ids=f"get_daily_data_{ticker}"),
            orient="index",
        ).T
        print(stock_df)
        stock_df = stock_df[["date", "symbol", "low", "high"]]
        dfs.append(stock_df)
    df = pd.concat(dfs, axis=0)
    pc = PostgresClient(DB_NAME, POSTGRES_USER, POSTGRES_PASS, DB_HOST, DB_PORT)
    try:
        pc.insert_from_frame(df, "stock")
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")


def get_stock_report(**context):
    pc = PostgresClient(DB_NAME, POSTGRES_USER, POSTGRES_PASS, DB_HOST, DB_PORT)
    df = pc.to_frame("SELECT * FROM STOCK")
    print("DataFrame a Plotear",df)   
   # df.plot()
    #plt.show()
    matplotlib.pyplot.ioff()
    df["low"] = pd.to_numeric(df["low"])
    ax = df.hist(column=['low'])  # s is an instance of Series
    fig = ax[0][0].get_figure()
    fig.savefig('dags/figure.pdf')
    
    print(os.getcwd())
     

default_args = {"owner": "lmattar", "retries": 0}
with DAG(
    "stock_dag",
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2021, 10, 11),
    catchup=False,
    default_args=default_args,
) as dag:

    t_if_not_exists_create_tables = PythonOperator(
        task_id="if_not_exists_create_tables",
        python_callable=if_not_exists_create_tables,
    )

    # Crea varias tareas en un for concatenando el nombre de la empresa al id de la tarea.
    get_data_task = {}
    for company, symbol in STOCKS.items():
        get_data_task[company] = PythonOperator(
            task_id=f"get_daily_data_{company}",
            python_callable=get_stock_data,
            op_args=[symbol],
        )

    tsk_insert = PythonOperator(
        task_id="_insert_stock_in_database",
        python_callable=_insert_stock_in_database,
    )

    t_report = PythonOperator(
        task_id="get_stock_report",
        python_callable=get_stock_report,
    )
    t_report.set_upstream(t_if_not_exists_create_tables)

    for company in STOCKS:
        upstream_task = t_if_not_exists_create_tables
        task = get_data_task[company]
        upstream_task.set_downstream(task)
        task.set_downstream(tsk_insert)
