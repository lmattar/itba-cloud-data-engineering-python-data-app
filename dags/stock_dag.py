"""Dummy DAG."""
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
import requests
import sqlalchemy
from  postgresClient import PostgresClient

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator



def pintar():
    print("funcion PINTAR")

BASE_URL = 'https://www.alphavantage.co/query'
API_KEY = 'PCQVY7EY4NVW7AGD' # mi api key =
STOCK_FN = 'TIME_SERIES_DAILY'
DB_NAME = 'stockdb'
DB_HOST = 'pg_container'
DB_PORT = '5432'
POSTGRES_USER = 'postgres'
POSTGRES_PASS = 'postgres'
STOCKS = {'apple': 'aapl', 'tesla': 'tsla', 'facebook': 'fb'}

def get_stock_data(stock_symbol, **context):
    
    date = context["ds"] #f"{date:%Y-%m-%d}"  # read execution date from context
    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")
  
    # r = requests.get(end_point)
    # data = json.loads(r.content)
    data = {'date': ['07/11/2021'], 'symbol': [stock_symbol], 'low':['10'], 'high':['30'] }  
    df = pd.DataFrame(data)
    # df = (
    #     pd.DataFrame(data)
    #     .T.reset_index()
    #     .rename(columns={'index': 'date'})
    # )
    
    #df = df[df['date'] == date]
    
     
    print("el data frame",df)
    
    return df.to_json()

def insert_stock_in_database(**context):
    task_instance = context["ti"]
    data = [task_instance.xcom_pull(task_ids="get_stock_data")]
    df = pd.DataFrame(data, columns=["date", "symbol", "low", "high"])
    print(f"salida-->{df}")
    pc = PostgresClient(DB_NAME,POSTGRES_USER,POSTGRES_PASS,DB_HOST,DB_PORT)
    pc.insert_from_frame(df,"stock")
    print(pc.to_frame('SELECT * FROM stock'))
    
def _insert_stock_in_database(**context):
    task_instance = context['ti']
    # Get xcom for each upstream task
    dfs = []
    for ticker in STOCKS:
        print("salida-->",f'get_daily_data_{ticker}')
        stock_df =  pd.read_json(
            task_instance.xcom_pull(task_ids=f'get_daily_data_{ticker}'),
            orient='index',
        ).T
        print(stock_df)
        stock_df = stock_df[['date', 'symbol', 'low', 'high']]
        dfs.append(stock_df)
    df = pd.concat(dfs, axis=0)
    pc = PostgresClient(DB_NAME,POSTGRES_USER,POSTGRES_PASS,DB_HOST,DB_PORT)
    try:
        pc.insert_from_frame(df, 'stock')
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")


with DAG(
    "stock",
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2021, 10, 11),
    catchup=False,
) as dag:
    
    
    tsk_get_stock_data = PythonOperator(
                            task_id="get_stock_data",
                            python_callable=get_stock_data,
                            op_args=["aapl"]
                               )

    # Create several task in loop
    get_data_task = {}
    for company, symbol in STOCKS.items():
        get_data_task[company] = PythonOperator(
            task_id=f'get_daily_data_{company}',
            python_callable= get_stock_data,
            op_args=[symbol],
        )
    
    tsk_insert = PythonOperator(
                            task_id="_insert_stock_in_database",
                            python_callable=_insert_stock_in_database,
                            
    )


    for company in STOCKS:
        #upstream_task = create_table_if_not_exists
        task = get_data_task[company]
        #upstream_task.set_downstream(task)
        task.set_downstream(tsk_insert)
    

 