import pandas as pd
from ingestion.db import load_to_staging, truncate_table
from ingestion.utils import normalize_df

DATA_PATH = "/opt/airflow/data_raw/marketing/"

def ingest_campaign_data():
    truncate_table("stg_campaign_data")

    df = pd.read_csv(DATA_PATH + "campaign_data.csv", sep="\t")

    expected = [
        "index_raw",
        "campaign_id",
        "campaign_name",
        "campaign_description",
        "discount"
    ]

    df = normalize_df(df, expected)
    load_to_staging(df, "stg_campaign_data")


def ingest_transactional_campaign_data():
    truncate_table("stg_transactional_campaign")

    df = pd.read_csv(DATA_PATH + "transactional_campaign_data.csv")

    expected = [
        "index_raw",
        "transaction_date",
        "campaign_id",
        "order_id",
        "estimated arrival",
        "availed"
    ]

    df = normalize_df(df, expected)
    load_to_staging(df, "stg_transactional_campaign")
