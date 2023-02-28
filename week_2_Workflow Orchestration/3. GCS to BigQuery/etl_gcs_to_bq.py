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
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passanger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passanger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df:pd.DataFrame) -> None:
    """Write DataFrame to BQ"""
    
    gcp_credentials_block = GcpCredentials.load("zoom-gcp")
    
    df.to_gbq(destination_table='datacamp.rides',
              project_id='datacamp-378414',
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=500_000, 
              if_exists='append')
 

@flow()
def etl_gcs_to_bq():
    """This ETL main function to load data into bigquery"""
    color='yellow'
    year=2021
    month=1
    
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    etl_gcs_to_bq()