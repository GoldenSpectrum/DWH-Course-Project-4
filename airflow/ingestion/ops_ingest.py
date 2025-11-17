import pandas as pd
from ingestion.db import load_to_staging

DATA_PATH = "/opt/airflow/data_raw/operations/"

def clean_quantity(col):
    return col.str.extract("(\d+)").astype(int)


def ingest_line_item_prices():
    # 3 files, different formats
    files = [
        "line_item_data_prices1.csv",
        "line_item_data_prices2.csv",
        "line_item_data_prices3.parquet"
    ]

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
            df = df.drop(columns=["Unnamed: 0"])
        else:
            df = pd.read_parquet(DATA_PATH + file)

        df["quantity"] = clean_quantity(df["quantity"])

        load_to_staging(df, "stg_line_item_prices")


def ingest_line_item_products():
    files = [
        "line_item_data_products1.csv",
        "line_item_data_products2.csv",
        "line_item_data_products3.parquet"
    ]

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
            df = df.drop(columns=["Unnamed: 0"])
        else:
            df = pd.read_parquet(DATA_PATH + file)

        load_to_staging(df, "stg_line_item_products")


def ingest_order_data():
    files = [
        "order_data_20211001-20220101.csv",
        "order_data_20200701-20211001.pickle",
        "order_data_20220101-20221201.xlsx",
        "order_data_20221201-20230601.json",
        "order_data_20200101-20200701.parquet",
        "order_data_20230601-20240101.html"
    ]

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
            df = df.drop(columns=["Unnamed: 0"])
        elif file.endswith(".pickle"):
            df = pd.read_pickle(DATA_PATH + file)
        elif file.endswith(".xlsx"):
            df = pd.read_excel(DATA_PATH + file)
        elif file.endswith(".json"):
            df = pd.read_json(DATA_PATH + file)
        elif file.endswith(".parquet"):
            df = pd.read_parquet(DATA_PATH + file)
        else:  # HTML
            df = pd.read_html(DATA_PATH + file)[0]
            df = df.drop(columns=["Unnamed: 0"])

        df["estimated_arrival_days"] = (
            df["estimated arrival"].str.extract("(\d+)").astype(int)
        )

        load_to_staging(df, "stg_order_data")


def ingest_order_delays():
    df = pd.read_html(DATA_PATH + "order_delays.html")[0]
    df = df.drop(columns=["Unnamed: 0"])
    load_to_staging(df, "stg_order_delays")
