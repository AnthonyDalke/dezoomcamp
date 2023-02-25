import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

# services = ['fhv','green','yellow']
init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dezoomcamp-week3")


def upload_to_gcs(
    bucket, 
    object_name, 
    local_file
    ):

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(
    year, 
    service
    ):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv'

        # download it using requests via a pandas df
        request_url = init_url + file_name
        r = requests.get(request_url)
        pd.DataFrame(io.StringIO(r.text)).to_csv(file_name)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name)
        file_name = file_name.replace(
            '.csv', 
            '.parquet'
            )
        df.to_parquet(
            file_name, 
            engine='pyarrow'
            )
        print(f"Parquet: {file_name}")

        # upload it to gcs
        upload_to_gcs(
            BUCKET, 
            f"{service}/{file_name}", 
            file_name
            )
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'fhv')
web_to_gcs('2020', 'fhv')