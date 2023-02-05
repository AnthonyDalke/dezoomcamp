from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dezoomcamp-375102", 
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

    
@flow(log_prints=True)
def etl_gcs_to_bq(
    color: str="yellow", 
    year: int=2019, 
    months: list[int]=[2, 3]
):
    """Main ETL flow to load data into BigQuery"""
    row_count = 0

    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        row_count = row_count + len(df)
        
        write_bq(df)
    
    print(f"Count of rows processed by script: {row_count}")

if __name__ == "__main__":
    months = [2, 3]
    year = 2019
    color = "yellow"

    etl_gcs_to_bq(color, year, months)