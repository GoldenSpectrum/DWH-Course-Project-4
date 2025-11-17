import pandas as pd
from ingestion.db import load_to_staging

DATA_PATH = "/opt/airflow/data_raw/marketing/"

def ingest_campaign_data():
    # This file is TAB-separated but saved wrong
    df = pd.read_csv(DATA_PATH + "campaign_data.csv", sep="\t")

    load_to_staging(df, "stg_campaign_data")


def ingest_transactional_campaign_data():
    df = pd.read_csv(DATA_PATH + "transactional_campaign_data.csv")
    df = df.drop(columns=["Unnamed: 0"])
    
    # fix estimated arrival (string -> integer)
    df["estimated_arrival_days"] = (
        df["estimated arrival"].str.extract("(\d+)").astype(int)
    )

    load_to_staging(df, "stg_transactional_campaign")
