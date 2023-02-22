from pathlib import Path, PurePosixPath
import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    print(f"df size: {len(df)}")
    return df

@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """"Fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """"Write DataDFrame locally as parquet file"""
    pre_path = Path.cwd().parent
    path = Path(f"{pre_path}/data/{color}/{dataset_file}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    df.to_parquet(path, compression="gzip")
    print(f'Procced {len(df)} lines')
    return path

@task()
def write_gcs(path: Path) -> None:
    """"Uploading local parquet file to GCS"""

    gcs_block = GcsBucket.load("zoom-gcs-bucket")

    to_p = f"data/{path.parent.stem}/{path.name}"
    gcs_block.upload_from_path(
        from_path=path,
        to_path=to_p
    )
    return 

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL Function"""

    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()