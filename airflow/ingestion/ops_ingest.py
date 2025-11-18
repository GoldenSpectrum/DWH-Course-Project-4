import pandas as pd
from ingestion.db import load_to_staging, truncate_table
from ingestion.utils import normalize_df

DATA_PATH = "/opt/airflow/data_raw/operations/"

def ingest_line_item_prices():
    truncate_table("stg_line_item_prices")

    files = [
        "line_item_data_prices1.csv",
        "line_item_data_prices2.csv",
        "line_item_data_prices3.parquet"
    ]

    expected = ["index_raw", "order_id", "price", "quantity"]

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
        else:
            df = pd.read_parquet(DATA_PATH + file)

        df = normalize_df(df, expected)
        load_to_staging(df, "stg_line_item_prices")


def ingest_line_item_products():
    truncate_table("stg_line_item_products")

    files = [
        "line_item_data_products1.csv",
        "line_item_data_products2.csv",
        "line_item_data_products3.parquet"
    ]

    expected = ["index_raw", "order_id", "product_name", "product_id"]

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
        else:
            df = pd.read_parquet(DATA_PATH + file)

        df = normalize_df(df, expected)
        load_to_staging(df, "stg_line_item_products")


def ingest_order_data():
    truncate_table("stg_order_data")

    files = [
        "order_data_20211001-20220101.csv",
        "order_data_20200701-20211001.pickle",
        "order_data_20220101-20221201.xlsx",
        "order_data_20221201-20230601.json",
        "order_data_20200101-20200701.parquet",
        "order_data_20230601-20240101.html"
    ]

    expected = [
        "index_raw",
        "order_id",
        "user_id",
        "estimated arrival",
        "transaction_date"
    ]

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
        elif file.endswith(".pickle"):
            df = pd.read_pickle(DATA_PATH + file)
        elif file.endswith(".xlsx"):
            df = pd.read_excel(DATA_PATH + file)
        elif file.endswith(".json"):
            df = pd.read_json(DATA_PATH + file)
        elif file.endswith(".parquet"):
            df = pd.read_parquet(DATA_PATH + file)
        else:
            df = pd.read_html(DATA_PATH + file)[0]

        df = normalize_df(df, expected)
        load_to_staging(df, "stg_order_data")


def ingest_order_delays():
    truncate_table("stg_order_delays")

    df = pd.read_html(DATA_PATH + "order_delays.html")[0]

    expected = ["index_raw", "order_id", "delay in days"]

    df = normalize_df(df, expected)
    load_to_staging(df, "stg_order_delays")
