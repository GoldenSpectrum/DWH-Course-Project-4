import pandas as pd
from ingestion.db import load_to_staging, truncate_table

DATA_PATH = "/opt/airflow/data_raw/business/"

def ingest_product_list():
    truncate_table("stg_product_list")

    df = pd.read_excel(DATA_PATH + "product_list.xlsx")

    # Clean columns
    df = df.rename(columns={
        "Unnamed: 0": "index_raw",
        "product_id": "product_id",
        "product_name": "product_name",
        "product_type": "product_type",
        "price": "price"
    })
    df["product_type"] = df["product_type"].fillna("Unknown")

    load_to_staging(df, "stg_product_list")
