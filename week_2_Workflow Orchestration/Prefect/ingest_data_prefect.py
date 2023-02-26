#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import urllib.request
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url, workdirectory):
    os.chdir(workdirectory)
        
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    # download file
    urllib.request.urlretrieve(csv_url, filename=os.getcwd() + f'\\data\\{csv_name}')
    
    # read table
    df_iter = pd.read_csv('data\\' + csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    # transform data
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missig passanger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missig passanger count: {df['passenger_count'].isin([0]).sum()}")
    return df
    
@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    # we created block in UI and now we can load it and put here
    connection_block = SqlAlchemyConnector.load("postgres")
    # we used context manager for use connection to postgres. After finish work with Postgres, resourse will free
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name='SubFlow', log_prints=True)
def log_subflow(table_name:str):
    print(f'logging subflow for: {table_name}')

@flow(name="Ingest Flow")
def main_flow(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    workdirectory = 'C:\\Users\\Дмитрий\\WorkFolder\\Программирование\\GitHub\\dataeng-zoomcamp\\week_2_Workflow Orchestration\\Prefect'


    log_subflow(table_name)
    raw_data = extract_data(csv_url, workdirectory)
    data = transform_data(raw_data)
    ingest_data(table_name, data)

if __name__ == '__main__':
    main_flow(table_name="yellow_taxi_trips3")