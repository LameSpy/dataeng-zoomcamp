from pathlib import Path # useful library for work fith path in os
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from datetime import timedelta

@task(retries=3)
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """"Download trip data from gcs"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=Path(f'{Path.cwd()}'))
    return Path(f'{Path.cwd()}/{gcs_path}')

@task()
def transform(path:Path) -> pd.DataFrame:
    """Function just read the parquet file"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df:pd.DataFrame, schema_name: str, table_name:str) -> int:
    """Write DataFrame to BQ"""
    
    gcp_credentials_block = GcpCredentials.load("zoom-gcp")
    
    df.to_gbq(destination_table=f'{schema_name}.{table_name}',
              project_id='datacamp-378414',
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=500_000, 
              if_exists='append')
    rows_of_df = df.shape[0]
    return rows_of_df
 

@flow()
def etl_gcs_to_bq(color:str, year:int, month:int, schema_name:str, table_name:str) -> int:
    """This ETL main function to load data into bigquery""" 
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    row_count = write_bq(df, schema_name, table_name)
    return row_count

@flow(log_prints=True)
def etl_parent_flow(months: list[int]=[1,2], year:int = 2021, color:str = 'yellow', schema_name:str = 'datacamp', table_name:str='temp'):
    """This function use flow etl_gcs_to_bq but it also has loop for proccessing many files"""
    total_rows = 0
    for month in months:
      rows = etl_gcs_to_bq(color, year, month, schema_name, table_name)
      total_rows = total_rows + rows
    
    print(f'Total proccessed rows is: {total_rows}')


if __name__ == '__main__':
    months = [2,3]
    year = 2019
    color = 'yellow'
    table_name = f'rides_{year}'
    etl_parent_flow(months, year, color, table_name=table_name)