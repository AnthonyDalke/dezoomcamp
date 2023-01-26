#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "yellow_tripdata_2021-01.csv.gz"

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True: 
        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print("inserted another chunk, took %.3f second" % (t_end - t_start))
        except StopIteration:
            print("Ingestion complete.")
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres.")

    parser.add_argument("--user", help="User name for Postgres")
    parser.add_argument("--password", help="Password for Postgres")
    parser.add_argument("--host", help="Host for Postgres")
    parser.add_argument("--port", help="Port for Postgres")
    parser.add_argument("--db", help="DB name for Postgres")
    parser.add_argument("--table_name", help="Name of table receiving data")
    parser.add_argument("--url", help="URL of CSV file")

    args = parser.parse_args()

    main(args)