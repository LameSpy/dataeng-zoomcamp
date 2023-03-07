from pathlib import Path # useful library for work fith path in os
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket


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
    df['DOlocationID'] = df.DOlocationID.astype("Int64")
    df['PUlocationID'] = df.PUlocationID.astype("Int64")
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    print('I work from github')
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file:str) -> Path:
    """Write dataframe out locally as parquet file"""
    path = Path(f'{Path.cwd()}/data/{dataset_file}.parquet')
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path:Path, dataset_file:str) -> None:
    """Uploading local parquet file to Google cloud storage"""
    gcp_bucket = GcsBucket.load("zoom-gcs")
    gcp_bucket.upload_from_path(from_path = path, to_path =  f'data/fhv/{dataset_file}.parquet')
    return 

@flow()
def etl_web_to_gcs(year:int, month:int) -> None: # this way we describe that result will be none
    """This ETL main function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path, dataset_file)

@flow()
def etl_parent_flow(months: list[int]=[1,2], year:int = 2019):
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == "__main__":
    months=list(range(1, 13))
    year=2019
    etl_parent_flow(months, year)