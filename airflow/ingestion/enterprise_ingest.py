import pandas as pd
from ingestion.db import load_to_staging

DATA_PATH = "/opt/airflow/data_raw/enterprise/"

def ingest_merchant_data():
    df = pd.read_html(DATA_PATH + "merchant_data.html")[0]
    df = df.drop(columns=["Unnamed: 0"])
    load_to_staging(df, "stg_merchant_data")


def ingest_staff_data():
    df = pd.read_html(DATA_PATH + "staff_data.html")[0]
    df = df.drop(columns=["Unnamed: 0"])
    load_to_staging(df, "stg_staff_data")


def ingest_order_with_merchant1():
    df = pd.read_parquet(DATA_PATH + "order_with_merchant_data1.parquet")
    load_to_staging(df, "stg_order_with_merchant")


def ingest_order_with_merchant2():
    df = pd.read_parquet(DATA_PATH + "order_with_merchant_data2.parquet")
    load_to_staging(df, "stg_order_with_merchant")


def ingest_order_with_merchant3():
    df = pd.read_csv(DATA_PATH + "order_with_merchant_data3.csv")
    df = df.drop(columns=["Unnamed: 0"])
    load_to_staging(df, "stg_order_with_merchant")
