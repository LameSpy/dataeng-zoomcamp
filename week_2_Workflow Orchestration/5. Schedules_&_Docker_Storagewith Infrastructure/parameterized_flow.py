from pathlib import Path # useful library for work fith path in os
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
import numpy as np


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url:str) -> pd.DataFrame:
    """Read tax data from web into to pandas DataFrame"""

    """ if randint(0,1) > 0: This block only for test to call exception and see how work retries
        raise Exception  """
    
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issue"""
    """ df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df["passenger_count"] = df.passenger_count.astype("int64") """
    df1 = df.fillna(np.nan)
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    print('I work from github')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file:str) -> Path:
    """Write dataframe out locally as parquet file"""
    path = Path(f'{Path.cwd()}/data/{color}/{dataset_file}.parquet')
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression='gzip')
    return path

@task(retries=3, retry_delay_seconds=5)
def write_gcs(path:Path, color:str, dataset_file:str) -> None:
    """Uploading local parquet file to Google cloud storage"""
    gcp_bucket = GcsBucket.load("zoom-gcs")
    gcp_bucket.upload_from_path(from_path = path, to_path =  f'data/{color}/{dataset_file}.parquet', timeout=120)
    return 

@flow()
def etl_web_to_gcs(year:int, month:int, color:str) -> None: # this way we describe that result will be none
    """This ETL main function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)

@flow()
def etl_parent_flow(months: list[int]=[1,2], years:list[int] = [2021], colors:list[str] = ['yellow']):
    for color in colors:
        for year in years:
            for month in months:
                etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    colors=['yellow']
    months=[1,2,3,4,5,6,7,8,9,10,11,12]
    years=[2020]
    etl_parent_flow(months, years, colors)